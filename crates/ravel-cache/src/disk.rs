//! Local-disk tier of ADR-0046's read cache (docs/adrs/0046-read-cache-tier.md,
//! decision 3's disk half, decision 4, decision 6, decision 7, and rejected
//! alternative 6). Content-addressed raw-byte-range storage under a
//! configured directory, keyed by the existing [`CacheKey`].
//!
//! This is the first production file Ravel writes (ADR-0046's survey found
//! none outside test code and two startup/CLI conveniences). Object storage
//! stays the only durable backend: this tier is opt-in (no directory
//! configured, no disk tier -- callers that never construct a [`DiskCache`]
//! see byte-for-byte the same behavior as before this crate existed), and
//! every failure mode degrades to a cache miss rather than an error. There
//! is structurally no way to return an error from [`DiskCache::get`]: its
//! return type is `Option<Bytes>`, not a `Result`, so a corrupt, truncated,
//! foreign, or missing entry has no error variant available to surface
//! through even by a future bug. `insert` is `()`: the caller already has
//! its own copy of the bytes it is offering to cache, so admission failing
//! silently loses nothing durable.
//!
//! **Crash safety: write-to-temp-name-then-rename, not a checksum-only
//! read.** [`DiskCache::insert`] writes the full header-plus-payload buffer
//! to a scratch path in the same shard directory, then renames it onto the
//! final content-addressed path. `rename(2)` on the same filesystem is
//! atomic: any process that opens the final path either finds nothing (the
//! old miss it already tolerates) or finds every byte the writer produced,
//! never a byte range that stops partway through a write. A process killed
//! mid-write leaves an orphaned temp file that no reader ever looks at
//! through its content-addressed path, but the startup scan (and every
//! later scan, since a fresh `DiskCache` can be constructed against a
//! directory another instance is still using) sweeps it: any file in a
//! shard directory that is not a live entry at its own canonical path is
//! deleted rather than left to accumulate.
//!
//! **Per-instance namespace subdirectory** (issue #671). Every entry a
//! [`DiskCache`] writes lives under a namespace subdirectory of the configured
//! directory: `dir/<namespace>/<shard>/<file>`, never `dir/<shard>/<file>`.
//! The namespace is fixed at construction ([`DiskCache::new_in_namespace`]);
//! [`DiskCache::new`] applies [`DEFAULT_NAMESPACE`]. This is the isolation two
//! instances over one `--cache-dir` need: the server builds one `DiskCache`
//! for the fetcher cache and a second for the catalog byte cache over the same
//! directory, and without a namespace they share one file namespace, so
//! on-disk bytes can reach ~2x the configured `max_bytes`, either instance's
//! eviction deletes files the other still counts resident, and a restart's
//! `scan_existing` seeds both with the union of both instances' files. A
//! namespace scopes all four operations to `dir/<namespace>`: `path_for`
//! writes there, `scan_existing` reads only there, the age sweep walks only
//! there, and eviction unlinks only there. Distinct labels ("store",
//! "catalog") therefore never see, count, or delete each other's files.
//!
//! A directory warmed by a build from *before* this change holds entries at
//! the old rootless `dir/<shard>/<file>` layout. Those files sit outside every
//! namespace subdirectory, so `scan_existing` (which reads only
//! `dir/<namespace>`) neither seeds nor deletes them, and no instance counts
//! them toward its byte or entry budget: they are inert. They are not a leak
//! in the accounting sense (no live entry points at them and no budget hides
//! them), but they do occupy disk until an operator deletes the stale root
//! shards or a future full-directory sweep reclaims them; this crate's
//! per-namespace scan deliberately does not, because deleting files outside
//! its own namespace would race a concurrent instance that owns the sibling
//! namespace under the same root.
//!
//! **Read-time verification is crc32c, and that is the whole of it**
//! (decision 4, amended 2026-08-02). `CacheKey` is `(tenant_hash,
//! content_hash, offset, len)`, where `content_hash` is the blake3 of the
//! *whole object* an entry is a byte sub-range of, and the key carries no
//! object size -- so this crate has no way to identify the full-object case
//! where `blake3(payload) == content_hash` would even be checkable, and the
//! original "verify blake3 once, on admission" design this crate shipped
//! with turned out to be unimplementable: the hash it computed and stored
//! was never anything readers could check the payload against, so no code
//! path ever read it back. What actually protects a disk entry, stated so
//! no later reader assumes more:
//!
//! - **Corruption after a successful write** (bit rot, a hand-edited or
//!   otherwise damaged file) is caught by a crc32c over the payload,
//!   recomputed on every hit.
//! - **A foreign or stale file at an entry's path** (a previous release's
//!   incompatible format, a file this cache never wrote) is caught by
//!   comparing all four key fields in the entry header against the
//!   requested key.
//! - **Bytes that were never the named range to begin with** are caught by
//!   nothing here, or anywhere else in the tree. Such bytes pass this
//!   cache's crc32c and pass every crc32c in the segment reader's
//!   footer/section/page/block hierarchy, and would produce silently wrong
//!   query results. Closing that gap is the admitting funnel's obligation,
//!   not this crate's: the funnel holds both the `SegmentRef` a range came
//!   from and the bytes themselves, and must not admit a payload under a
//!   key that does not describe it.
//!
//! **Plaintext, no encryption inside Ravel** (decision 7). Cached bytes are
//! written to disk exactly as received, with no cipher. **With SSE-KMS
//! configured, cached bytes on local disk are not protected by that key.**
//! A deployment that needs encryption at rest for this directory supplies
//! it at the filesystem layer, the same precedent ADR-0042 already set for
//! object storage.
//!
//! **Eviction is S3-FIFO** (decision 6, amended 2026-08-02), the same
//! policy and the same implementation the RAM tier uses (see [`crate::s3fifo`]):
//! this tier instantiates `S3Fifo<()>`, since the payload itself lives on
//! disk rather than in the eviction structure, and passes the entry size in
//! explicitly. Scan resistance matters *more* here than in RAM: a disk miss
//! costs an S3 fetch, the single most expensive thing a query does, and
//! this is the large tier that actually holds the working set. A plain FIFO
//! would let one compaction or fold pass evict everything queries rely on;
//! S3-FIFO's probation queue absorbs a scan without ever touching entries
//! already promoted to main.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::runtime::Handle;

use crate::clock::{Clock, SystemClock};
use crate::key::CacheKey;
use crate::limits::CacheLimits;
use crate::metrics::CacheMetrics;
use crate::s3fifo::S3Fifo;

const MAGIC: [u8; 4] = *b"RVCD";
// Version 3 adds the 8-byte `written_at_ns` stamp (ADR-0064).
// Bumping the version is what makes a version-2 entry (no stamp) written by an
// older binary decode-fail here rather than be misread at shifted offsets or,
// worse, served fresh forever with no age to check: an old entry is rejected
// by `decode_header`'s version gate, discarded, and re-fetched. That is the
// deterministic, fail-safe handling deliverable 2 requires.
const FORMAT_VERSION: u32 = 3;
const HEADER_LEN: usize = 4 + 4 + 16 + 32 + 8 + 8 + 8 + 4;
const ENTRY_EXTENSION: &str = "rvc";

/// Namespace label [`DiskCache::new`]/[`DiskCache::new_with_clock`] apply when
/// the caller does not name one (issue #671). Every instance lives under a
/// namespace subdirectory of the configured directory, so an un-labelled
/// `new` call still lands in its own file namespace rather than sharing the
/// directory root with another instance. Two instances that must share one
/// `--cache-dir` (the server's fetcher cache and catalog byte cache) pass
/// distinct labels through [`DiskCache::new_in_namespace`] instead of relying
/// on this default; two instances both built through the default `new` would
/// still collide, which is why production paths name their label.
const DEFAULT_NAMESPACE: &str = "default";

/// Process-wide counter so two `DiskCache` instances constructed in the
/// same process (even pointed at the same directory) never derive the same
/// scratch filename: the temp path includes both the OS pid and this
/// instance id, where the pid alone only disambiguates across processes.
static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);

struct Header {
    tenant_hash: [u8; 16],
    content_hash: [u8; 32],
    offset: u64,
    len: u64,
    /// Wall-clock time the entry was written, nanoseconds since the Unix
    /// epoch, from the injected [`Clock`]. Read on every hit and by the
    /// startup scan to age the entry out (ADR-0064). Not covered
    /// by `crc32c` (which is payload-only, unchanged): a corrupted stamp only
    /// ever makes an entry look older (an immediate, safe miss) or younger,
    /// and a younger-looking entry is still bounded by S3-FIFO eviction, so no
    /// corruption of these 8 bytes can extend an entry's life without bound.
    written_at_ns: u64,
    crc32c: u32,
}

fn encode_header(key: &CacheKey, payload: &[u8], written_at_ns: u64) -> [u8; HEADER_LEN] {
    let mut buf = [0u8; HEADER_LEN];
    let mut pos = 0usize;
    buf[pos..pos + 4].copy_from_slice(&MAGIC);
    pos += 4;
    buf[pos..pos + 4].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    pos += 4;
    buf[pos..pos + 16].copy_from_slice(&key.tenant_hash);
    pos += 16;
    buf[pos..pos + 32].copy_from_slice(&key.content_hash);
    pos += 32;
    buf[pos..pos + 8].copy_from_slice(&key.offset.to_le_bytes());
    pos += 8;
    buf[pos..pos + 8].copy_from_slice(&key.len.to_le_bytes());
    pos += 8;
    buf[pos..pos + 8].copy_from_slice(&written_at_ns.to_le_bytes());
    pos += 8;
    buf[pos..pos + 4].copy_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    buf
}

/// Parses a header, or returns `None` for anything that is not exactly a
/// well-formed header this format produced: short reads, bad magic, and an
/// unknown version all collapse to the same "not a usable entry" outcome,
/// because the caller's only two responses are "use it" or "discard it".
/// The version check also means a header this same code wrote under the
/// previous, wider layout (with a trailing blake3 field no reader ever
/// consumed) is rejected here rather than misread at the wrong offsets.
fn decode_header(buf: &[u8; HEADER_LEN]) -> Option<Header> {
    let mut pos = 0usize;
    let magic: [u8; 4] = buf[pos..pos + 4].try_into().ok()?;
    pos += 4;
    if magic != MAGIC {
        return None;
    }
    let version = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
    pos += 4;
    if version != FORMAT_VERSION {
        return None;
    }
    let tenant_hash: [u8; 16] = buf[pos..pos + 16].try_into().ok()?;
    pos += 16;
    let content_hash: [u8; 32] = buf[pos..pos + 32].try_into().ok()?;
    pos += 32;
    let offset = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let len = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let written_at_ns = u64::from_le_bytes(buf[pos..pos + 8].try_into().ok()?);
    pos += 8;
    let crc32c = u32::from_le_bytes(buf[pos..pos + 4].try_into().ok()?);
    Some(Header {
        tenant_hash,
        content_hash,
        offset,
        len,
        written_at_ns,
        crc32c,
    })
}

fn push_hex(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
}

/// Deterministic, collision-free path for `key` under `dir`: a two-hex-char
/// shard prefix taken from `content_hash` (directory fan-out, so one busy
/// tenant does not put thousands of files in a single directory), then a
/// filename built from every field of `key` verbatim, so distinct keys can
/// never map to the same path.
fn path_for(dir: &Path, key: &CacheKey) -> PathBuf {
    let mut shard = String::with_capacity(2);
    push_hex(&mut shard, &key.content_hash[..1]);
    let mut name = String::with_capacity(16 + 64 + 16 + 16 + 4);
    push_hex(&mut name, &key.tenant_hash);
    name.push('-');
    push_hex(&mut name, &key.content_hash);
    name.push('-');
    push_hex(&mut name, &key.offset.to_be_bytes());
    name.push('-');
    push_hex(&mut name, &key.len.to_be_bytes());
    dir.join(shard).join(name).with_extension(ENTRY_EXTENSION)
}

/// The shared, `Arc`-held state of a [`DiskCache`]: everything the cache and
/// its background age-sweep task both touch. Split out from [`DiskCache`] so
/// the sweeper can hold a [`Weak`] to it rather than a strong reference: the
/// sweeper never keeps the tier alive, so dropping the [`DiskCache`] frees this
/// state and the next tick sees a dead `Weak` and exits (see [`spawn_sweeper`]).
struct Inner {
    /// The namespaced root every path operation derives from:
    /// `configured_dir/<namespace>` (issue #671). `path_for`, `scan_existing`,
    /// the age sweep, and eviction all read and write only beneath this, so a
    /// sibling instance's `configured_dir/<other-namespace>` is never touched.
    dir: PathBuf,
    limits: CacheLimits,
    state: Mutex<S3Fifo<()>>,
    metrics: Arc<CacheMetrics>,
    tmp_counter: AtomicU64,
    instance_id: u64,
    /// Injected wall clock. Stamped into each entry's header at `insert`, read
    /// on every `get`, and read by the periodic sweep to age entries out past
    /// `limits.max_entry_age_ns` (ADR-0064). Injected so tests
    /// drive ageing deterministically instead of sleeping.
    clock: Arc<dyn Clock>,
}

/// The disk tier of ADR-0046's read cache. See the [module docs](self) for
/// the crash-safety, verification, encryption, and eviction rationale.
///
/// A per-`get` age check and the startup scan drop entries past
/// `max_entry_age_ns`, but only ones that are read or that a fresh process
/// scans. An entry that is never re-read and sees no eviction pressure would
/// keep its bytes on disk indefinitely; a background sweep (ADR-0064)
/// closes that gap by dropping every over-age entry on a
/// fixed interval, so an idle entry ages out within one
/// `limits.sweep_interval_ns` past `max_entry_age_ns`.
pub struct DiskCache {
    inner: Arc<Inner>,
    /// Handle to the background age-sweep task. Always present: construction
    /// requires a Tokio runtime (see [`DiskCache::new_with_clock`]), so the
    /// sweep can never be silently skipped. Aborted on drop so no sweeper task
    /// outlives its cache.
    sweeper: tokio::task::JoinHandle<()>,
}

impl DiskCache {
    /// Opens (or begins populating) a disk tier rooted at `dir`, which need
    /// not exist yet: a missing directory is created lazily on the first
    /// successful insert, and is never required for the cache to be
    /// constructed or read from (deliverable 5 -- a node whose cache
    /// directory is deleted mid-flight must go on answering queries
    /// correctly, and that starts with construction itself never failing).
    ///
    /// A best-effort scan seeds byte and entry accounting from whatever is
    /// already on disk (a previous process's entries surviving a restart).
    /// Anything the scan cannot recognise as a live entry at its own
    /// canonical path -- a previous release's incompatible layout, an
    /// orphaned `.tmp-*` scratch file left by a crash mid-insert, a foreign
    /// file, or a valid-looking entry sitting at the wrong path -- is
    /// deleted on sight rather than counted: discard and rebuild, never
    /// repair, applies at startup exactly as it does on a live read.
    ///
    /// This also spawns the background age-sweep task (ADR-0064), which
    /// requires a Tokio runtime. Must be constructed inside
    /// one; constructing off a runtime panics rather than silently skipping the
    /// sweep. See [`DiskCache::new_with_clock`].
    pub fn new(dir: PathBuf, limits: CacheLimits) -> Self {
        Self::new_in_namespace_with_clock(dir, DEFAULT_NAMESPACE, limits, Arc::new(SystemClock))
    }

    /// Like [`DiskCache::new`], but with an explicit [`Clock`] for the
    /// per-entry max-age and the background sweep (ADR-0064).
    /// Production uses [`DiskCache::new`], which injects a [`SystemClock`];
    /// tests inject a clock they advance across the max-age boundary by hand.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime. The background age-sweep is a
    /// hard requirement of the tier's erasure-residue bound (ADR-0064), not an
    /// optional extra: a construction that could not spawn it must fail loudly,
    /// exactly as [`tokio::spawn`] itself would, rather than leave the sweep
    /// un-spawned and the bound silently unenforced for idle entries.
    pub fn new_with_clock(dir: PathBuf, limits: CacheLimits, clock: Arc<dyn Clock>) -> Self {
        Self::new_in_namespace_with_clock(dir, DEFAULT_NAMESPACE, limits, clock)
    }

    /// Like [`DiskCache::new`], but with an explicit `namespace` label
    /// (issue #671). Every entry is stored under `dir/<namespace>/...` and the
    /// startup scan, age sweep, and eviction all scope to `dir/<namespace>`, so
    /// two instances over one directory with distinct labels never see, count,
    /// or delete each other's files. `namespace` must be a single non-empty
    /// path component. The server passes `"store"` for the fetcher cache
    /// (`services/ravel-server/src/store.rs`); a `"catalog"` label for the
    /// catalog byte cache is planned and not yet wired, so today only one
    /// namespace is in use. See the
    /// [module docs](self) for the warm-directory migration behavior.
    pub fn new_in_namespace(dir: PathBuf, namespace: &str, limits: CacheLimits) -> Self {
        Self::new_in_namespace_with_clock(dir, namespace, limits, Arc::new(SystemClock))
    }

    /// Like [`DiskCache::new_in_namespace`], but with an explicit [`Clock`].
    /// The single constructor every other entry point funnels through.
    ///
    /// # Panics
    ///
    /// Panics if called outside a Tokio runtime, exactly as
    /// [`DiskCache::new_with_clock`] does and for the same reason.
    ///
    /// Panics if `namespace` is not a single non-empty path segment. This is
    /// a runtime check, not a debug assertion, because the failure it prevents
    /// is silent: `Path::join("")` yields the base directory unchanged, which
    /// would restore the shared root this namespacing exists to remove and let
    /// two instances evict each other again, and a leading-slash or multi-
    /// segment label escapes the intended layout. Every call site passes a
    /// literal, so a violation is a programmer error at construction.
    pub fn new_in_namespace_with_clock(
        dir: PathBuf,
        namespace: &str,
        limits: CacheLimits,
        clock: Arc<dyn Clock>,
    ) -> Self {
        // Exactly one ordinary component. A string check is not enough: on
        // Windows a drive prefix like "C:" or "C:foo" contains no separator yet
        // makes `PathBuf::join` REPLACE the base path rather than extend it,
        // which escapes the layout as surely as a leading slash does. Asking
        // `Path::components` settles every case with one rule. A Windows drive
        // prefix is included: there "C:foo" yields Prefix + Normal and is
        // rejected, while on Unix the same string is one ordinary filename that
        // escapes nothing and is correctly accepted. That is why there is no
        // cross-platform test for it -- asserting the rejection on Unix would
        // assert a falsehood.
        let mut parts = std::path::Path::new(namespace).components();
        let one_normal =
            matches!(parts.next(), Some(std::path::Component::Normal(_))) && parts.next().is_none();
        assert!(
            one_normal,
            "DiskCache namespace must be exactly one ordinary path component, got {namespace:?}"
        );
        // Acquire the runtime handle first, so an off-runtime construction fails
        // before doing any filesystem work rather than after.
        let handle = Handle::current();
        // Bake the namespace into the root every path operation derives from, so
        // `path_for`, `scan_existing`, the sweep, and eviction are all scoped to
        // `dir/<namespace>` with no further namespace threading (issue #671).
        let dir = dir.join(namespace);
        let state = Inner::scan_existing(&dir, limits, clock.now_ns());
        let inner = Arc::new(Inner {
            dir,
            limits,
            state: Mutex::new(state),
            metrics: Arc::new(CacheMetrics::default()),
            tmp_counter: AtomicU64::new(0),
            instance_id: NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            clock,
        });
        let sweeper = spawn_sweeper(&inner, &handle);
        DiskCache { inner, sweeper }
    }

    /// A cloneable handle to this tier's counters, independent of the
    /// cache's own lifetime.
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        self.inner.metrics.clone()
    }

    /// Current number of resident entries this process knows about.
    pub fn len(&self) -> usize {
        self.inner.state.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Current total bytes across every resident entry this process knows
    /// about.
    pub fn total_bytes(&self) -> u64 {
        self.inner.state.lock().total_bytes()
    }

    /// Look up `key`. Every failure -- the directory or file missing, a
    /// permission error, a short read, a bad header, a key mismatch, an
    /// oversized entry, an over-age entry, or a crc32c mismatch -- returns
    /// `None`. There is no error variant this method can return: that is
    /// enforced by its signature, not by a convention callers must remember.
    pub fn get(&self, key: &CacheKey) -> Option<Bytes> {
        self.inner.get(key)
    }

    /// Admit `value` under `key`. Never an error and never partially visible;
    /// see [`Inner::insert`] and the [module docs](self) for the crash-safety
    /// and rejection rules.
    pub fn insert(&self, key: CacheKey, value: &[u8]) {
        self.inner.insert(key, value);
    }

    /// Runs one age sweep synchronously: every entry older than
    /// `max_entry_age_ns` is dropped, exactly as the background task does on
    /// each tick. The background task is the production driver; this is the
    /// deterministic hook a test uses to trigger a single sweep without waiting
    /// on a timer.
    #[cfg(test)]
    fn sweep_expired_now(&self) {
        self.inner.sweep_expired();
    }

    /// A `Weak` to the shared state, for tests that assert the sweeper leaks no
    /// strong reference: after the cache is dropped this must not upgrade.
    #[cfg(test)]
    fn inner_weak(&self) -> Weak<Inner> {
        Arc::downgrade(&self.inner)
    }

    /// The namespaced root this instance stores entries under
    /// (`configured_dir/<namespace>`), for tests that compute an entry's
    /// canonical path with [`path_for`] or inspect the on-disk layout: after
    /// namespacing (issue #671) the files live here, not directly under the
    /// directory passed to the constructor.
    #[cfg(test)]
    fn dir(&self) -> &Path {
        &self.inner.dir
    }
}

impl Drop for DiskCache {
    /// Aborts the background sweep task so it never outlives the cache. Even
    /// without this, the sweeper holds only a [`Weak`] and would exit on its
    /// next tick once this drop frees the last strong reference; the abort just
    /// makes shutdown prompt rather than up to one interval late.
    fn drop(&mut self) {
        self.sweeper.abort();
    }
}

/// Spawns the periodic age-sweep task for `inner` on `handle`, returning its
/// join handle. Non-optional: the caller supplies a runtime [`Handle`], so
/// there is no off-runtime path that silently returns nothing. The task holds
/// only a [`Weak`], so it never keeps the tier alive: once the owning
/// [`DiskCache`] drops, the next `upgrade` fails and the task exits. It also
/// holds no strong reference across its `await`, so a drop that lands while the
/// task is parked between ticks frees the state immediately.
fn spawn_sweeper(inner: &Arc<Inner>, handle: &Handle) -> tokio::task::JoinHandle<()> {
    let weak: Weak<Inner> = Arc::downgrade(inner);
    // Zero would panic `interval`; a sane floor keeps a misconfigured or
    // test-shrunk value from doing so while staying deterministic.
    let period = Duration::from_nanos(inner.limits.sweep_interval_ns.max(1));
    handle.spawn(async move {
        let mut ticker = tokio::time::interval(period);
        // The first tick fires immediately; consume it so the first real sweep
        // is one interval out, not at construction time.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match weak.upgrade() {
                // Scoped so the strong reference is dropped at the end of this
                // statement, before the next `tick().await`.
                Some(inner) => inner.sweep_expired(),
                None => break,
            }
        }
    })
}

impl Inner {
    fn scan_existing(dir: &Path, limits: CacheLimits, now_ns: u64) -> S3Fifo<()> {
        let mut fifo = S3Fifo::new(limits);
        let Ok(shards) = fs::read_dir(dir) else {
            return fifo;
        };
        for shard in shards.flatten() {
            let Ok(file_type) = shard.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Ok(entries) = fs::read_dir(shard.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                match Self::scan_one(&path) {
                    Some((key, size, written_at_ns))
                        if path_for(dir, &key) == path
                            && now_ns.saturating_sub(written_at_ns) <= limits.max_entry_age_ns =>
                    {
                        fifo.seed(key, (), size);
                    }
                    _ => {
                        // Not recognisable as a *live, unexpired* entry at its
                        // own canonical path: a foreign file, a previous
                        // format, an orphaned scratch file (a `.tmp-*` name
                        // has no parseable header at all), a well-formed entry
                        // that simply lives at the wrong path (which would
                        // otherwise leak its real file uncounted and
                        // double-count a duplicate key into `total_bytes`), or
                        // an entry already older than the max-age. This
                        // startup scan is the "existing eviction walk" ADR-0064
                        // names: an aged-out entry is deleted here rather than
                        // seeded, so restarting a node never resurrects bytes
                        // that were due to be dropped. Delete rather than count.
                        let _ = fs::remove_file(&path);
                    }
                }
            }
        }
        fifo
    }

    /// Reads just the header of a candidate file and cross-checks the
    /// declared payload length against the file's actual physical size, so
    /// a file truncated between a crash and the next startup is discarded
    /// here rather than accounted for and trusted until the next read.
    fn scan_one(path: &Path) -> Option<(CacheKey, u64, u64)> {
        let metadata = fs::metadata(path).ok()?;
        let mut file = fs::File::open(path).ok()?;
        let mut header_buf = [0u8; HEADER_LEN];
        file.read_exact(&mut header_buf).ok()?;
        let header = decode_header(&header_buf)?;
        let physical_payload = metadata.len().checked_sub(HEADER_LEN as u64)?;
        if physical_payload != header.len {
            return None;
        }
        let key = CacheKey::new(
            header.tenant_hash,
            header.content_hash,
            header.offset,
            header.len,
        );
        Some((key, header.len, header.written_at_ns))
    }

    /// Look up `key`, the shared-state half of [`DiskCache::get`]. Every
    /// failure returns `None`; there is no error variant.
    fn get(&self, key: &CacheKey) -> Option<Bytes> {
        let path = path_for(&self.dir, key);
        match self.read_and_verify(key, &path) {
            Some(bytes) => {
                // Record the touch for S3-FIFO promotion. The entry is
                // already known-resident (verification just re-read it
                // from disk); this only affects eviction ordering.
                self.state.lock().get(key);
                self.metrics.record_hit(bytes.len() as u64);
                Some(bytes)
            }
            None => {
                self.metrics.record_miss();
                None
            }
        }
    }

    fn read_and_verify(&self, key: &CacheKey, path: &Path) -> Option<Bytes> {
        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(err) => {
                // A clean "nothing at this path" miss (the common case: the
                // key was never cached, or a prior `discard` already removed
                // it) is not a disk error. Anything else opening a path this
                // cache's own layout produced -- permission denied, a broken
                // mount, too many open files -- is the disk tier being
                // unhealthy, not the cache being cold, so it is counted
                // separately (see `CacheMetrics::record_disk_error`).
                if err.kind() != std::io::ErrorKind::NotFound {
                    self.metrics.record_disk_error();
                }
                // Forget any accounting for this key so a later insert of
                // the identical key is not refused as "already resident"
                // forever (the scenario ADR-0046 requires a query to
                // survive -- the cache directory disappearing mid-flight).
                self.discard(key, path);
                return None;
            }
        };
        let mut header_buf = [0u8; HEADER_LEN];
        if file.read_exact(&mut header_buf).is_err() {
            // Shorter than even a header: cannot be a complete entry.
            self.metrics.record_disk_error();
            self.discard(key, path);
            return None;
        }
        let Some(header) = decode_header(&header_buf) else {
            // Bad magic/version: a previous release's format, or a file
            // this cache never wrote at all.
            self.metrics.record_disk_error();
            self.discard(key, path);
            return None;
        };
        if header.tenant_hash != key.tenant_hash
            || header.content_hash != key.content_hash
            || header.offset != key.offset
            || header.len != key.len
        {
            // Parses, but names a different key: never something a correct
            // writer of this format would produce at this path, since the
            // path is derived from the same fields.
            self.metrics.record_disk_error();
            self.discard(key, path);
            return None;
        }
        if header.len > self.limits.max_entry_bytes {
            // Admitted under looser limits before a config change, or hand
            // -placed by a test: never served past the current maximum.
            self.metrics.record_disk_error();
            self.discard(key, path);
            return None;
        }
        if self.clock.now_ns().saturating_sub(header.written_at_ns) > self.limits.max_entry_age_ns {
            // Older than the configured max-age: an erased subject's bytes
            // must not outlive the sweep on local disk by more than this
            // (ADR-0064). Dropped and re-fetched, not served. This
            // is not corruption -- the entry is well-formed and its crc32c
            // would pass -- so it is an expiry, not a `record_disk_error`, and
            // the payload is not even read. `saturating_sub` keeps a clock
            // that jumped backward (a stamp newer than "now") from wrapping
            // into a huge age: such an entry is simply treated as not yet
            // expired.
            self.metrics.record_expired_max_age();
            self.discard(key, path);
            return None;
        }
        let mut payload = vec![0u8; header.len as usize];
        if file.read_exact(&mut payload).is_err() {
            // Header is intact but the payload is short: truncated, either
            // by a crash this format's rename discipline should have
            // prevented, or by damage after the rename.
            self.metrics.record_disk_error();
            drop(file);
            self.discard(key, path);
            return None;
        }
        if crc32c::crc32c(&payload) != header.crc32c {
            self.metrics.record_disk_error();
            drop(file);
            self.discard(key, path);
            return None;
        }
        Some(Bytes::from(payload))
    }

    fn discard(&self, key: &CacheKey, path: &Path) {
        let _ = fs::remove_file(path);
        self.state.lock().remove(key);
    }

    /// Admit `value` under `key`. Never an error and never partially
    /// visible: `value` is written to a scratch path and renamed onto the
    /// content-addressed final path, so a reader only ever sees nothing or
    /// everything (see the crash-safety discussion in the [module
    /// docs](self)). Any failure along the way -- the directory missing and
    /// uncreatable, read-only, out of space, or anything else -- leaves no
    /// trace and is not reported: the caller already has its own copy of
    /// `value`, so nothing is lost by declining to cache it.
    ///
    /// Rejects `value` outright, without touching disk, if its length
    /// disagrees with `key.len`: the header records `key.len` as the
    /// payload length, so admitting a mismatched `value` would store a
    /// self-contradictory entry that can only ever miss on every future
    /// read while still counting as a successful admission.
    fn insert(&self, key: CacheKey, value: &[u8]) {
        let size = value.len() as u64;
        if size != key.len {
            self.metrics.record_rejected_size();
            return;
        }
        if size > self.limits.max_entry_bytes {
            self.metrics.record_rejected_size();
            return;
        }
        if self.state.lock().contains(&key) {
            // Content-addressed: an existing entry for this key is already
            // these exact bytes.
            return;
        }

        let path = path_for(&self.dir, &key);
        let Some(parent) = path.parent() else {
            return;
        };
        if fs::create_dir_all(parent).is_err() {
            return;
        }
        let tmp_path = self.tmp_path_for(parent);
        let written_at_ns = self.clock.now_ns();
        let mut buf = Vec::with_capacity(HEADER_LEN + value.len());
        buf.extend_from_slice(&encode_header(&key, value, written_at_ns));
        buf.extend_from_slice(value);
        if fs::write(&tmp_path, &buf).is_err() {
            let _ = fs::remove_file(&tmp_path);
            return;
        }
        if fs::rename(&tmp_path, &path).is_err() {
            let _ = fs::remove_file(&tmp_path);
            return;
        }

        let evicted = {
            let mut state = self.state.lock();
            let (_, evicted) = state.insert(key, (), size, &self.metrics);
            evicted
        };
        for evicted_key in evicted {
            let _ = fs::remove_file(path_for(&self.dir, &evicted_key));
        }
    }

    fn tmp_path_for(&self, parent: &Path) -> PathBuf {
        let id = self.tmp_counter.fetch_add(1, Ordering::Relaxed);
        parent.join(format!(
            ".tmp-{}-{}-{id}",
            std::process::id(),
            self.instance_id
        ))
    }

    /// Drops every entry whose stamped write time is older than
    /// `max_entry_age_ns`, regardless of whether it was ever re-read. This is
    /// the periodic driver the background task calls on each tick
    /// (ADR-0064): the per-`get` age check only reaches entries
    /// that are read, and the startup scan only runs at construction, so an
    /// idle entry under no eviction pressure needs this walk to have its bytes
    /// physically removed within one sweep interval past the max-age.
    ///
    /// The walk mirrors the startup scan but is deliberately narrower: it
    /// deletes *only* files that parse as a live entry at their own canonical
    /// path and are over-age. Anything else -- a `.tmp-*` scratch file a
    /// concurrent `insert` is mid-rename on, a foreign file, a misplaced entry
    /// -- is left untouched, because a running cache must not race an in-flight
    /// insert; the startup scan owns cleaning those up. Deleting the file and
    /// dropping the entry from S3-FIFO accounting is idempotent against a
    /// concurrent `get`/`insert`: the state lock serialises the accounting
    /// change, and a redundant `remove_file` on an already-gone path is a
    /// tolerated miss.
    fn sweep_expired(&self) {
        let now_ns = self.clock.now_ns();
        let Ok(shards) = fs::read_dir(&self.dir) else {
            return;
        };
        for shard in shards.flatten() {
            let Ok(file_type) = shard.file_type() else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            let Ok(entries) = fs::read_dir(shard.path()) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some((key, _size, written_at_ns)) = Self::scan_one(&path)
                    && path_for(&self.dir, &key) == path
                    && now_ns.saturating_sub(written_at_ns) > self.limits.max_entry_age_ns
                {
                    // Over-age idle entry: delete the bytes and drop the
                    // accounting. `saturating_sub` keeps a backward clock jump
                    // (a stamp newer than "now") from wrapping into a huge age,
                    // matching the per-`get` check.
                    let _ = fs::remove_file(&path);
                    self.state.lock().remove(&key);
                    self.metrics.record_expired_max_age();
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    fn test_key(n: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([9u8; 16], content_hash, 0, 0)
    }

    fn test_key_with_len(n: u64, len: u64) -> CacheKey {
        let mut content_hash = [0u8; 32];
        content_hash[..8].copy_from_slice(&n.to_le_bytes());
        CacheKey::new([9u8; 16], content_hash, 0, len)
    }

    fn generous_limits() -> CacheLimits {
        CacheLimits::new(64 * 1024 * 1024, 10_000, 16 * 1024 * 1024)
    }

    /// A hand-advanced clock so the max-age boundary is crossed
    /// deterministically, without sleeping. `now_ns` reads whatever the test
    /// last set; `set` moves wall time forward (or back).
    struct TestClock(AtomicU64);

    impl TestClock {
        fn new(start_ns: u64) -> Arc<Self> {
            Arc::new(TestClock(AtomicU64::new(start_ns)))
        }
        fn set(&self, now_ns: u64) {
            self.0.store(now_ns, Ordering::Relaxed);
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    #[cfg(unix)]
    fn make_readonly(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o555)).unwrap();
    }

    #[cfg(unix)]
    fn make_writable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Whether this process walks through permission bits: root, or any uid
    /// holding `CAP_DAC_OVERRIDE`. Probed by writing into a `0o000` directory
    /// rather than read off a uid, because bypassing DAC is the property the
    /// mode-bit phases actually depend on, and it is not the same question as
    /// "is the caller uid 0" in either direction.
    #[cfg(unix)]
    fn bypasses_dac() -> bool {
        use std::os::unix::fs::PermissionsExt;
        let probe = TempDir::new().unwrap();
        let denied = probe.path().join("no-permissions");
        fs::create_dir(&denied).unwrap();
        fs::set_permissions(&denied, fs::Permissions::from_mode(0o000)).unwrap();
        let bypassed = fs::write(denied.join("probe"), b"x").is_ok();
        // Hand the directory back its own permissions so `TempDir`'s drop can
        // actually remove it; that drop swallows the error it would otherwise
        // hit, leaving the tree behind.
        fs::set_permissions(&denied, fs::Permissions::from_mode(0o700)).unwrap();
        bypassed
    }

    #[tokio::test]
    async fn round_trip_identical_bytes_all_zero_and_zero_length() {
        let tmp = TempDir::new().unwrap();
        let cache = DiskCache::new(tmp.path().to_path_buf(), generous_limits());

        let key_normal = test_key_with_len(1, 5);
        cache.insert(key_normal, b"hello");
        assert_eq!(cache.get(&key_normal).as_deref(), Some(b"hello".as_slice()));

        let key_zero_len = test_key_with_len(2, 0);
        cache.insert(key_zero_len, b"");
        assert_eq!(cache.get(&key_zero_len).as_deref(), Some(b"".as_slice()));

        let all_zero = vec![0u8; 4096];
        let key_all_zero = test_key_with_len(3, all_zero.len() as u64);
        cache.insert(key_all_zero, &all_zero);
        assert_eq!(
            cache.get(&key_all_zero).as_deref(),
            Some(all_zero.as_slice())
        );
    }

    /// Issue #703: every phase here must induce its failure at any uid. A
    /// read-only mode bit does not: root and any `CAP_DAC_OVERRIDE` caller
    /// write straight through one, so a phase that locks a directory and then
    /// asserts a miss asserts nothing there, and worse, fails, because the
    /// insert it expected to be dropped succeeds. The first two phases below
    /// therefore deny writes with ENOTDIR (a path component that is a regular
    /// file), which no uid can walk through; the one claim ENOTDIR cannot
    /// express keeps the mode bit and is skipped under a DAC bypass.
    ///
    /// FLIP (non-vacuity): in `Inner::insert`, replace
    /// `if fs::create_dir_all(parent).is_err() { return; }` with
    /// `fs::create_dir_all(parent).unwrap();`. The ENOTDIR phases then panic
    /// inside `insert` instead of degrading, and the test fails at any uid.
    #[tokio::test]
    async fn every_disk_failure_degrades_to_a_miss() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_path_buf();

        // Directory absent, and not creatable: the path component that would
        // hold it is a regular file, so `create_dir_all` cannot make the
        // configured directory exist and every write beneath it fails with
        // ENOTDIR. Reads and inserts must both behave like an ordinary miss,
        // never panic or report an error.
        {
            let blocking_file = base.join("a-regular-file");
            fs::write(&blocking_file, b"not a directory").unwrap();
            let missing_dir = blocking_file.join("cache-dir");
            let cache = DiskCache::new(missing_dir.clone(), generous_limits());
            let payload: &[u8] = b"payload";
            let key = test_key_with_len(1, payload.len() as u64);
            assert!(cache.get(&key).is_none());
            cache.insert(key, payload); // must not panic
            assert!(cache.get(&key).is_none());
            assert!(
                !missing_dir.exists(),
                "nothing may come into existence beneath a regular file"
            );
        }

        // Directory replaced by a regular file after an entry was cached: a
        // later insert's `create_dir_all`, and every write under the
        // configured path, now fail with ENOTDIR. What this phase can honestly
        // claim once the directory is gone is the degradation itself: the new
        // key is silently not admitted, and the key cached beforehand becomes
        // a clean miss rather than a panic or an error. The stronger claim
        // that a previously written entry stays *readable* needs the directory
        // to survive while denying writes, so it lives in the mode-bit phase
        // below.
        {
            let dir = base.join("replaced-by-file");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let cached_before_payload: &[u8] = b"cached before the swap";
            let already_cached = test_key_with_len(10, cached_before_payload.len() as u64);
            cache.insert(already_cached, cached_before_payload);
            assert!(cache.get(&already_cached).is_some());

            let moved_away = base.join("replaced-by-file-moved");
            fs::rename(&dir, &moved_away).unwrap();
            let replacement: &[u8] = b"a regular file where the cache directory was";
            fs::write(&dir, replacement).unwrap();

            let never_cached_payload: &[u8] = b"should not persist";
            let never_cached = test_key_with_len(11, never_cached_payload.len() as u64);
            cache.insert(never_cached, never_cached_payload);
            assert!(cache.get(&never_cached).is_none());
            assert!(
                cache.get(&already_cached).is_none(),
                "the cached entry's bytes went with the directory: a miss, not an error"
            );
            assert_eq!(
                fs::read(&dir).unwrap().as_slice(),
                replacement,
                "the cache must not have written through the file blocking its path"
            );
        }

        // The one claim ENOTDIR cannot express: an entry written while the
        // directory was writable stays readable once the directory itself is
        // read-only, and only the later insert is dropped. That needs a
        // directory that survives and denies writes, which is a mode bit and
        // nothing else. A caller that bypasses DAC walks through the bit, so
        // there the phase is skipped with a reason rather than asserting
        // something untrue of it (#703).
        #[cfg(unix)]
        {
            if bypasses_dac() {
                println!(
                    "skipping the read-only-directory phase: this process bypasses DAC \
                     (root, or CAP_DAC_OVERRIDE), so a read-only mode bit denies it nothing"
                );
            } else {
                let dir = base.join("readonly-dir");
                fs::create_dir_all(&dir).unwrap();
                let cache = DiskCache::new(dir.clone(), generous_limits());
                let cached_before_payload: &[u8] = b"cached before lockdown";
                let already_cached = test_key_with_len(12, cached_before_payload.len() as u64);
                cache.insert(already_cached, cached_before_payload);
                assert!(cache.get(&already_cached).is_some());

                // Lock the namespaced root (where entries actually live), not
                // the configured dir: after #671 a new shard is created under
                // `dir/<namespace>`, so that is the directory whose writes must
                // be denied for this phase to test anything.
                make_readonly(cache.dir());
                let never_cached_payload: &[u8] = b"should not persist";
                let never_cached = test_key_with_len(13, never_cached_payload.len() as u64);
                let never_cached_path = path_for(cache.dir(), &never_cached);
                let never_cached_shard = never_cached_path.parent().unwrap();
                assert!(
                    !never_cached_shard.exists(),
                    "precondition: this insert must need a new shard directory, \
                     which is what the read-only parent denies"
                );
                cache.insert(never_cached, never_cached_payload);
                assert!(cache.get(&never_cached).is_none());
                assert_eq!(
                    cache.get(&already_cached).as_deref(),
                    Some(cached_before_payload),
                    "an entry written while writable stays readable under a read-only directory"
                );
                make_writable(cache.dir());
            }
        }

        // Entry truncated mid-file: valid header, payload cut short.
        {
            let dir = base.join("truncated");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(20, 100);
            let payload = vec![7u8; 100];
            cache.insert(key, &payload);
            assert!(cache.get(&key).is_some());

            let path = path_for(cache.dir(), &key);
            let full = fs::read(&path).unwrap();
            fs::write(&path, &full[..full.len() - 10]).unwrap();
            assert!(cache.get(&key).is_none());
        }

        // Entry whose crc32c (the per-hit integrity check the header
        // carries) no longer matches the payload: a single flipped byte
        // after a clean write, simulating bit rot or on-disk damage.
        {
            let dir = base.join("bitrot");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(30, 64);
            let payload = vec![0xABu8; 64];
            cache.insert(key, &payload);
            assert!(cache.get(&key).is_some());

            let path = path_for(cache.dir(), &key);
            let mut full = fs::read(&path).unwrap();
            let last = full.len() - 1;
            full[last] ^= 0xFF;
            fs::write(&path, &full).unwrap();
            assert!(cache.get(&key).is_none());
        }

        // Entry over the configured maximum: insert() itself declines to
        // write it, so a lookup right after is a plain miss.
        {
            let dir = base.join("oversized");
            fs::create_dir_all(&dir).unwrap();
            let limits = CacheLimits::new(1024 * 1024, 100, 16);
            let cache = DiskCache::new(dir, limits);
            let key = test_key_with_len(40, 17);
            cache.insert(key, &[0u8; 17]);
            assert!(cache.get(&key).is_none());
            assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 1);
        }

        // Entry over the configured maximum, the other way: it was
        // admitted under looser limits (an earlier process, or a config
        // change), and is only too large under the limits this instance
        // was constructed with.
        {
            let dir = base.join("shrunk-limit");
            fs::create_dir_all(&dir).unwrap();
            let writer = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key_with_len(41, 4096);
            writer.insert(key, &vec![1u8; 4096]);
            assert!(writer.get(&key).is_some());

            let stricter = DiskCache::new(dir, CacheLimits::new(1024 * 1024, 100, 1024));
            assert!(stricter.get(&key).is_none());
        }

        // A garbage file this cache never wrote, sitting exactly at the
        // path a real key would map to.
        {
            let dir = base.join("garbage");
            fs::create_dir_all(&dir).unwrap();
            let cache = DiskCache::new(dir.clone(), generous_limits());
            let key = test_key(50);
            let path = path_for(cache.dir(), &key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, b"not a cache entry, just noise from somewhere else").unwrap();
            assert!(cache.get(&key).is_none());
        }
    }

    #[tokio::test]
    async fn partial_write_never_observed_as_complete() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let cache = DiskCache::new(dir.clone(), generous_limits());

        let key = test_key_with_len(1, 200);
        let payload = vec![0x42u8; 200];
        let full_header = encode_header(&key, &payload, 0);
        let path = path_for(cache.dir(), &key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        // Every truncation point short of the complete file: no bytes at
        // all, a partial header, exactly the header with no payload, and a
        // header with a partial payload. None of these is a real write
        // this cache ever performs (insert only ever exposes a complete
        // buffer via rename), but they are exactly what a crash mid-write
        // would leave behind if this cache wrote in place instead of via a
        // temp file, so the read path must refuse all of them regardless.
        let cutoffs = [0, 1, HEADER_LEN / 2, HEADER_LEN, HEADER_LEN + 50];
        for cutoff in cutoffs {
            let mut partial = Vec::with_capacity(cutoff);
            let mut full = Vec::with_capacity(HEADER_LEN + payload.len());
            full.extend_from_slice(&full_header);
            full.extend_from_slice(&payload);
            partial.extend_from_slice(&full[..cutoff.min(full.len())]);
            fs::write(&path, &partial).unwrap();
            assert!(
                cache.get(&key).is_none(),
                "a {cutoff}-byte partial file must never be observed as complete"
            );
        }
    }

    #[tokio::test]
    async fn deleting_cache_dir_while_live_keeps_reads_correct() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("cache");
        let cache = DiskCache::new(dir.clone(), generous_limits());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");
        assert_eq!(cache.get(&key).as_deref(), Some(b"hello".as_slice()));

        fs::remove_dir_all(&dir).unwrap();

        // Every subsequent read is a correct miss, never a panic or an
        // error: the cache directory disappearing is exactly the scenario
        // ADR-0046 requires a query to survive, just more slowly (the
        // caller falls back to object storage, which this crate does not
        // model, but "does not panic and reports a clean miss" is the
        // contract this crate owns).
        assert!(cache.get(&key).is_none());
        let other_key = test_key_with_len(2, 5);
        assert!(cache.get(&other_key).is_none());

        // The directory comes back on the next insert, and -- critically
        // -- the *same* key that just missed is insertable again: the
        // external deletion must not leave this key permanently wedged in
        // "already resident" accounting with no backing file.
        cache.insert(key, b"again");
        assert_eq!(cache.get(&key).as_deref(), Some(b"again".as_slice()));
        cache.insert(other_key, b"world");
        assert_eq!(cache.get(&other_key).as_deref(), Some(b"world".as_slice()));
    }

    #[tokio::test]
    async fn bounds_hold_under_insertion_pressure() {
        let tmp = TempDir::new().unwrap();
        let limits = CacheLimits::new(20 * 1024, 8, 4096);
        let cache = DiskCache::new(tmp.path().to_path_buf(), limits);

        for i in 0..200u64 {
            cache.insert(test_key_with_len(i, 512), &vec![0u8; 512]);
        }

        assert!(
            cache.len() <= 8,
            "entry-count bound violated: {} entries resident",
            cache.len()
        );
        assert!(
            cache.total_bytes() <= 20 * 1024,
            "byte bound violated: {} bytes resident",
            cache.total_bytes()
        );

        // The bound holds on disk too, not just in this process's
        // accounting: no more files are sitting in the directory than the
        // entry count allows.
        let mut on_disk = 0usize;
        for shard in fs::read_dir(cache.dir()).unwrap().flatten() {
            if shard.file_type().unwrap().is_dir() {
                on_disk += fs::read_dir(shard.path()).unwrap().count();
            }
        }
        assert!(
            on_disk <= 8,
            "{on_disk} files left on disk, entry bound is 8"
        );
    }

    #[tokio::test]
    async fn scan_at_startup_seeds_accounting_and_discards_junk() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        {
            let cache = DiskCache::new(dir.clone(), generous_limits());
            cache.insert(test_key_with_len(1, 10), &[1u8; 10]);
            cache.insert(test_key_with_len(2, 20), &[2u8; 20]);
        }

        // A junk file from some other source, sitting in the same shard
        // layout a real entry would use -- inside the instance's namespace
        // subdirectory, which is the only place its scan looks (#671).
        let junk_shard = dir.join(DEFAULT_NAMESPACE).join("zz");
        fs::create_dir_all(&junk_shard).unwrap();
        let junk_path = junk_shard.join("not-a-real-entry.rvc");
        let mut junk = fs::File::create(&junk_path).unwrap();
        junk.write_all(b"whatever this is, it is not ours").unwrap();
        drop(junk);

        let reopened = DiskCache::new(dir, generous_limits());
        assert_eq!(reopened.len(), 2);
        assert_eq!(reopened.total_bytes(), 30);
        assert!(
            !junk_path.exists(),
            "a startup scan must discard a file it cannot parse as its own format"
        );
        assert_eq!(
            reopened.get(&test_key_with_len(1, 10)).as_deref(),
            Some([1u8; 10].as_slice())
        );
        assert_eq!(
            reopened.get(&test_key_with_len(2, 20)).as_deref(),
            Some([2u8; 20].as_slice())
        );
    }

    /// Finding 3: `.tmp-<pid>-<instance>-<n>` scratch files left behind by
    /// a crash mid-insert have a leading-dot name with no other `.`, so
    /// `Path::extension()` returns `None` for them. A scan that filters on
    /// `Some("rvc")` never even looks at them, and they survive across
    /// restarts forever, silently exceeding the directory's configured
    /// byte bound. The fix inspects every file, not just `.rvc`-suffixed
    /// ones, and deletes anything it cannot parse as a live entry.
    #[tokio::test]
    async fn startup_scan_sweeps_orphaned_scratch_files() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // The orphan sits inside the instance's namespace subdirectory, the
        // only place its startup scan traverses (#671).
        let shard = dir.join(DEFAULT_NAMESPACE).join("ab");
        fs::create_dir_all(&shard).unwrap();
        let orphan = shard.join(format!(".tmp-{}-0-0", std::process::id()));
        fs::write(&orphan, vec![0u8; 4096]).unwrap();

        let cache = DiskCache::new(dir, generous_limits());
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.len(), 0);
        assert!(
            !orphan.exists(),
            "an orphaned scratch file must be swept at startup"
        );
    }

    /// Finding 4: `insert` must reject a payload whose length disagrees
    /// with `key.len`, rather than writing an entry the header itself
    /// contradicts (which then misses on every future read while still
    /// counting as an admission).
    #[tokio::test]
    async fn insert_rejects_length_mismatch_between_key_and_payload() {
        let tmp = TempDir::new().unwrap();
        let cache = DiskCache::new(tmp.path().to_path_buf(), generous_limits());

        let key = test_key_with_len(1, 999);
        cache.insert(key, b"much shorter than 999 bytes");

        assert!(cache.get(&key).is_none());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.metrics().snapshot().admissions_rejected_size, 1);
        assert_eq!(cache.metrics().snapshot().bytes_admitted, 0);
    }

    /// A key that was simply never cached is a clean miss: no entry ever
    /// existed at its canonical path, so nothing is unhealthy about the
    /// disk tier and `record_disk_error` must not fire.
    #[tokio::test]
    async fn get_on_never_cached_key_is_a_clean_miss_not_a_disk_error() {
        let tmp = TempDir::new().unwrap();
        let cache = DiskCache::new(tmp.path().to_path_buf(), generous_limits());

        assert!(cache.get(&test_key(1)).is_none());
        let snapshot = cache.metrics().snapshot();
        assert_eq!(snapshot.misses, 1);
        assert_eq!(
            snapshot.disk_errors_degraded_to_misses, 0,
            "a key that was never cached is not a disk error"
        );
    }

    /// A payload that fails its crc32c check on read (bit rot, a partial
    /// write outside this crate's own rename discipline) must degrade to a
    /// miss, per this module's doc, but that degraded miss is distinct from
    /// the plain "nothing at this path" case above: it is what
    /// `disk_errors_degraded_to_misses` exists to surface to an operator.
    #[tokio::test]
    async fn get_on_corrupted_payload_is_a_disk_error_not_a_clean_miss() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let cache = DiskCache::new(dir.clone(), generous_limits());

        let key = test_key_with_len(1, 64);
        let payload = vec![7u8; 64];
        cache.insert(key, &payload);
        assert!(cache.get(&key).is_some(), "precondition: entry is live");

        let path = path_for(cache.dir(), &key);
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        assert!(
            cache.get(&key).is_none(),
            "a corrupted entry must degrade to a miss, never an error"
        );
        let snapshot = cache.metrics().snapshot();
        assert_eq!(
            snapshot.disk_errors_degraded_to_misses, 1,
            "a corrupted payload is a disk error, not a clean miss"
        );
    }

    /// Finding 6: a well-formed entry file living at a path other than
    /// its own canonical `path_for` (a corrupted rename, a hand-placed
    /// file, or -- before this fix -- an orphaned pre-rename temp file
    /// that happened to be complete) must be deleted at startup, not
    /// counted: counting it would leak the real file at the canonical
    /// path uncounted and double-book its size into `total_bytes`.
    #[tokio::test]
    async fn startup_scan_deletes_misplaced_entry_rather_than_counting_it() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();

        let real_key = test_key_with_len(1, 64);
        let payload = vec![7u8; 64];
        let header = encode_header(&real_key, &payload, 0);
        // Inside the namespace subdirectory the scan traverses, but at a
        // non-canonical path within it (#671).
        let wrong_shard = dir.join(DEFAULT_NAMESPACE).join("zz");
        fs::create_dir_all(&wrong_shard).unwrap();
        let wrong_path = wrong_shard.join("misplaced.rvc");
        let mut buf = Vec::new();
        buf.extend_from_slice(&header);
        buf.extend_from_slice(&payload);
        fs::write(&wrong_path, &buf).unwrap();

        let cache = DiskCache::new(dir, generous_limits());
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.total_bytes(), 0);
        assert!(cache.get(&real_key).is_none());
        assert!(
            !wrong_path.exists(),
            "a misplaced entry file must be deleted, not counted"
        );
    }

    /// Finding 1: the disk tier now evicts with S3-FIFO, the same policy
    /// and implementation the RAM tier uses, so a scan of distinct
    /// once-touched keys must not evict a working set that was re-accessed
    /// after admission (a plain-FIFO disk tier fails this).
    #[tokio::test]
    async fn disk_tier_survives_repeated_scans_via_s3_fifo_eviction() {
        let tmp = TempDir::new().unwrap();
        let entry_size = 1024u64;
        let working_set_len = 20u64;
        let limits = CacheLimits::new(entry_size * 100, 1000, entry_size * 10);
        let cache = DiskCache::new(tmp.path().to_path_buf(), limits);

        let working_set: Vec<CacheKey> = (0..working_set_len)
            .map(|i| test_key_with_len(i, entry_size))
            .collect();
        for key in &working_set {
            cache.insert(*key, &vec![0xAAu8; entry_size as usize]);
        }
        // A second touch so S3-FIFO promotes each entry out of probation.
        for key in &working_set {
            assert!(cache.get(key).is_some());
        }

        for pass in 0..3u64 {
            let base = 1_000 + pass * 500;
            for i in base..base + 500 {
                cache.insert(
                    test_key_with_len(i, entry_size),
                    &vec![0xEEu8; entry_size as usize],
                );
            }
            let resident = working_set
                .iter()
                .filter(|key| cache.get(key).is_some())
                .count();
            assert!(
                resident as f64 >= working_set_len as f64 * 0.9,
                "working set collapsed on disk-tier scan pass {pass}: only {resident}/{working_set_len} resident"
            );
        }
    }

    /// ADR-0064: a disk entry younger than the max-age is served;
    /// once wall time crosses `written_at + max_age`, the same key is a miss
    /// and the stale bytes are dropped rather than served.
    ///
    /// This is the soundness proof for the max-age. To watch it bite, disable
    /// the age check in `read_and_verify` -- change its `>` to a comparison
    /// that is never true (e.g. `> u64::MAX`) or delete the whole
    /// `record_expired_max_age` block: the post-boundary `assert!(... is_none)`
    /// then fails because the aged-out bytes are served.
    #[tokio::test]
    async fn disk_entry_older_than_max_age_is_not_served() {
        let tmp = TempDir::new().unwrap();
        let max_age = 24 * 60 * 60 * 1_000_000_000u64; // 24h in ns
        let limits = generous_limits().with_max_entry_age_ns(max_age);
        let clock = TestClock::new(1_000_000);
        let cache = DiskCache::new_with_clock(tmp.path().to_path_buf(), limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");

        // Younger than max-age (well within the window): served.
        clock.set(1_000_000 + max_age / 2);
        assert_eq!(
            cache.get(&key).as_deref(),
            Some(b"hello".as_slice()),
            "an entry younger than the max-age must be served"
        );

        // Exactly at the boundary (age == max_age): still served, the check is
        // strictly-greater-than.
        clock.set(1_000_000 + max_age);
        assert_eq!(
            cache.get(&key).as_deref(),
            Some(b"hello".as_slice()),
            "an entry exactly at the max-age is not yet expired"
        );

        // One nanosecond past the boundary: a miss, the file is dropped, and
        // it is counted as an expiry rather than a corruption/disk error.
        clock.set(1_000_000 + max_age + 1);
        assert!(
            cache.get(&key).is_none(),
            "an entry older than the max-age must not be served"
        );
        let snapshot = cache.metrics().snapshot();
        assert_eq!(snapshot.disk_entries_expired_max_age, 1);
        assert_eq!(
            snapshot.disk_errors_degraded_to_misses, 0,
            "an expiry is not a disk error: the entry was well-formed"
        );
        assert!(
            !path_for(cache.dir(), &key).exists(),
            "the stale entry's bytes must be dropped from disk, not left to persist"
        );

        // A re-insert after expiry (the fetch that a real caller would do on
        // the miss) is admitted again and served, with a fresh stamp.
        cache.insert(key, b"hello");
        assert_eq!(cache.get(&key).as_deref(), Some(b"hello".as_slice()));
    }

    /// The startup scan is ADR-0064's "existing eviction walk": an entry
    /// already older than the max-age when a fresh `DiskCache` opens the
    /// directory is deleted and not counted, so a restart never resurrects
    /// bytes that were due to be dropped.
    #[tokio::test]
    async fn startup_scan_drops_entries_already_past_max_age() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let max_age = 1_000_000_000u64; // 1s in ns
        let limits = generous_limits().with_max_entry_age_ns(max_age);

        let writer_clock = TestClock::new(10_000_000_000);
        {
            let writer = DiskCache::new_with_clock(dir.clone(), limits, writer_clock.clone());
            writer.insert(test_key_with_len(1, 10), &[1u8; 10]);
        }

        // Reopen with a clock far past the entry's write time + max-age.
        let reader_clock = TestClock::new(10_000_000_000 + max_age + 1);
        let reopened = DiskCache::new_with_clock(dir.clone(), limits, reader_clock);
        assert_eq!(reopened.len(), 0, "an aged-out entry must not be seeded");
        assert_eq!(reopened.total_bytes(), 0);
        assert!(
            !path_for(reopened.dir(), &test_key_with_len(1, 10)).exists(),
            "the startup scan must delete an entry already past the max-age"
        );
    }

    /// Deliverable 2 / finding F2: an entry written by an older binary in the
    /// previous (version-2, no timestamp) header layout must not be served
    /// fresh forever. The version bump makes `decode_header` reject it, so it
    /// degrades to a miss and is discarded, never read as a headerless entry
    /// with an implicit zero age. Crafted by hand at the canonical path in the
    /// exact 76-byte v2 layout this code used before the stamp was added.
    ///
    /// This test uses a `TestClock` (not a real `SystemClock`) precisely so the
    /// miss isolates the *version gate*, independent of age. A v2 file's total
    /// length here is exactly `HEADER_LEN`, so the header read succeeds and the
    /// miss can only come from `decode_header` rejecting version 2 -- the
    /// header-version/short-read gate. The clock is pinned so the age check is
    /// provably not what fires: the assertions below require the expiry counter
    /// to stay zero (an age-driven miss would bump it) and the disk-error
    /// counter to be exactly the one version-gate rejection.
    #[tokio::test]
    async fn old_format_entry_without_timestamp_is_a_miss_not_served_forever() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        // A fixed clock, so the age comparison is deterministic and provably
        // not the cause of the miss below.
        let clock = TestClock::new(1_000_000);
        let cache = DiskCache::new_with_clock(dir.clone(), generous_limits(), clock);

        let key = test_key_with_len(1, 8);
        let payload = [0x11u8; 8];

        // The old 76-byte header: magic, version 2, the four key fields, then
        // a payload crc32c -- no written_at field. With an 8-byte payload the
        // file is exactly HEADER_LEN (84) bytes, so the header read is not
        // short: the sole reason it misses is the version-2 gate.
        let mut old_header = Vec::new();
        old_header.extend_from_slice(&MAGIC);
        old_header.extend_from_slice(&2u32.to_le_bytes());
        old_header.extend_from_slice(&key.tenant_hash);
        old_header.extend_from_slice(&key.content_hash);
        old_header.extend_from_slice(&key.offset.to_le_bytes());
        old_header.extend_from_slice(&key.len.to_le_bytes());
        old_header.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());
        assert_eq!(old_header.len(), 4 + 4 + 16 + 32 + 8 + 8 + 4); // 76, the v2 layout
        assert_eq!(old_header.len() + payload.len(), HEADER_LEN); // reads as a full header

        let path = path_for(cache.dir(), &key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut buf = old_header;
        buf.extend_from_slice(&payload);
        fs::write(&path, &buf).unwrap();

        assert!(
            cache.get(&key).is_none(),
            "a previous-format entry must degrade to a miss, never be served"
        );
        let snapshot = cache.metrics().snapshot();
        assert_eq!(
            snapshot.disk_entries_expired_max_age, 0,
            "the miss must come from the version gate, not the age check"
        );
        assert_eq!(
            snapshot.disk_errors_degraded_to_misses, 1,
            "a foreign/previous-format file is a disk error, and the only one here"
        );
    }

    /// Negative/robustness: a short file, a garbage file, and an
    /// almost-header-length file at an entry's canonical path each produce a
    /// clean miss under the max-age code path, never a panic or wrong bytes.
    /// The age check reads `written_at_ns` from the header, so a file too
    /// short to contain one must be rejected before that read is ever
    /// attempted.
    #[tokio::test]
    async fn short_and_garbage_entries_never_panic_under_age_check() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let clock = TestClock::new(5_000_000_000);
        let cache = DiskCache::new_with_clock(dir.clone(), generous_limits(), clock);

        let key = test_key_with_len(7, 32);
        let path = path_for(cache.dir(), &key);
        fs::create_dir_all(path.parent().unwrap()).unwrap();

        for bytes in [
            Vec::new(),
            vec![0u8; 3],
            vec![0xFFu8; HEADER_LEN - 1],
            b"RVCD not really a header at all........".to_vec(),
        ] {
            fs::write(&path, &bytes).unwrap();
            assert!(
                cache.get(&key).is_none(),
                "a {}-byte malformed entry must be a miss",
                bytes.len()
            );
        }
    }

    /// Soundness proof for the periodic sweep: an entry that is
    /// never re-read (no `get`, no eviction pressure) and ages past
    /// `max_entry_age_ns` must have its bytes physically removed from disk by
    /// the sweep, not linger until something happens to touch it. This is the
    /// idle case the per-`get` and startup checks never reach.
    ///
    /// The sweep is triggered directly (`sweep_expired_now`) so the proof is
    /// deterministic and does not depend on a timer; the spawned periodic task
    /// drives the same `Inner::sweep_expired` on each tick (see
    /// `spawned_sweeper_evicts_idle_entry_within_interval`).
    ///
    /// FLIP (non-vacuity): in `Inner::sweep_expired`, change
    /// `now_ns.saturating_sub(written_at_ns) > self.limits.max_entry_age_ns`
    /// to `... > u64::MAX` (never true). The sweep then deletes nothing, the
    /// idle file survives, and the `!path.exists()` assertion below fails.
    #[tokio::test]
    async fn idle_entry_past_max_age_is_swept_from_disk() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let max_age = 24 * 60 * 60 * 1_000_000_000u64; // 24h in ns
        let limits = generous_limits().with_max_entry_age_ns(max_age);
        let clock = TestClock::new(1_000_000);
        let cache = DiskCache::new_with_clock(dir.clone(), limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");
        let path = path_for(cache.dir(), &key);
        assert!(path.exists(), "precondition: the entry is on disk");
        assert_eq!(cache.len(), 1);

        // Never `get`-touched. Advance the injected clock past the max-age and
        // run exactly one sweep.
        clock.set(1_000_000 + max_age + 1);
        cache.sweep_expired_now();

        assert!(
            !path.exists(),
            "an idle aged-out entry's bytes must be physically gone after the sweep"
        );
        assert!(
            cache.get(&key).is_none(),
            "a swept entry is a subsequent miss"
        );
        assert_eq!(cache.len(), 0, "the sweep drops S3-FIFO accounting too");
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.metrics().snapshot().disk_entries_expired_max_age, 1);

        // An entry still within the max-age is left alone by the sweep: it is
        // not an indiscriminate purge.
        let fresh = test_key_with_len(2, 5);
        cache.insert(fresh, b"world");
        cache.sweep_expired_now();
        assert_eq!(
            cache.get(&fresh).as_deref(),
            Some(b"world".as_slice()),
            "a within-age entry must survive the sweep"
        );
    }

    /// Wiring proof: the background task spawned at construction
    /// drives `sweep_expired` on its interval, with no manual trigger. An idle
    /// entry aged past the max-age is gone within one sweep interval.
    #[tokio::test]
    async fn spawned_sweeper_evicts_idle_entry_within_interval() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let max_age = 1_000_000_000u64; // 1s in ns
        // A short sweep interval so the test does not wait on the 1 h default.
        let limits = generous_limits()
            .with_max_entry_age_ns(max_age)
            .with_sweep_interval_ns(5_000_000); // 5 ms
        let clock = TestClock::new(10_000_000_000);
        let cache = DiskCache::new_with_clock(dir.clone(), limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");
        let path = path_for(cache.dir(), &key);
        assert!(path.exists(), "precondition: the entry is on disk");

        // Age it out; the periodic task must notice on its own within a bounded
        // number of intervals.
        clock.set(10_000_000_000 + max_age + 1);

        let mut gone = false;
        for _ in 0..400 {
            if !path.exists() {
                gone = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            gone,
            "the periodic sweeper must physically remove an idle aged-out entry on its own"
        );
        assert!(cache.get(&key).is_none());
    }

    /// Clean shutdown: dropping the cache must leave no sweeper
    /// task holding its state alive. The sweeper holds only a `Weak`, so once
    /// the sole strong `Arc` (inside `DiskCache`) drops, the shared state is
    /// freed and a later `upgrade` returns `None`; `Drop` also aborts the task
    /// so it never wakes again.
    #[tokio::test]
    async fn sweeper_task_stops_when_cache_dropped() {
        let tmp = TempDir::new().unwrap();
        let limits = generous_limits().with_sweep_interval_ns(5_000_000);
        let clock = TestClock::new(1);
        let cache = DiskCache::new_with_clock(tmp.path().to_path_buf(), limits, clock);

        // The sweeper is non-optional now: construction requires a runtime, so
        // a spawned task always exists (its absence is unrepresentable).
        let weak = cache.inner_weak();
        assert!(weak.upgrade().is_some(), "precondition: state is live");

        drop(cache);

        // Yield so the aborted task is reaped; the state is freed regardless,
        // since the task never held a strong reference across its await.
        tokio::task::yield_now().await;
        assert!(
            weak.upgrade().is_none(),
            "dropping the cache must leave no live sweeper holding its state"
        );
    }

    /// `max_entry_age_ns == 0` must expire promptly with no u64
    /// underflow or panic in either age-comparison site (the per-`get` check
    /// and the sweep). The `saturating_sub` in both sites makes this safe;
    /// this test guards it.
    #[tokio::test]
    async fn max_entry_age_zero_expires_promptly_without_underflow() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let limits = generous_limits().with_max_entry_age_ns(0);
        let clock = TestClock::new(5_000_000);
        let cache = DiskCache::new_with_clock(dir.clone(), limits, clock.clone());

        let key = test_key_with_len(1, 5);
        cache.insert(key, b"hello");

        // Same instant: age is 0, and the boundary is strictly-greater-than, so
        // the entry is not yet expired. No underflow computing `now - written`.
        assert_eq!(
            cache.get(&key).as_deref(),
            Some(b"hello".as_slice()),
            "at age 0 the strictly-greater-than boundary still serves"
        );

        // Any forward step past the write instant expires it via the `get`
        // path, again with no underflow.
        clock.set(5_000_001);
        assert!(
            cache.get(&key).is_none(),
            "one ns past write, age 0 expires"
        );
        assert_eq!(cache.metrics().snapshot().disk_entries_expired_max_age, 1);

        // The sweep path handles `max_entry_age_ns == 0` identically: a fresh
        // idle entry one ns old is swept, no panic.
        cache.insert(key, b"hello"); // stamped at now = 5_000_001
        clock.set(5_000_002);
        cache.sweep_expired_now();
        assert!(
            !path_for(cache.dir(), &key).exists(),
            "the sweep must drop a one-ns-old entry when max-age is 0"
        );

        // The startup scan must also survive `max_entry_age_ns == 0` without
        // underflow: an entry written and immediately reopened one ns later is
        // dropped, not seeded.
        let writer_clock = TestClock::new(7_000_000);
        {
            let writer = DiskCache::new_with_clock(dir.clone(), limits, writer_clock);
            writer.insert(test_key_with_len(9, 5), b"world");
        }
        let reader = DiskCache::new_with_clock(dir, limits, TestClock::new(7_000_001));
        assert_eq!(reader.len(), 0, "age-0 startup scan drops the entry");
    }

    /// The sweeper's own graceful-exit path -- a tick that finds
    /// the `Weak` dead and `break`s out of the loop -- was previously
    /// unexercised (every existing test relied on `Drop::abort`, which stops
    /// the task before that branch can run). Here the task is spawned against an
    /// `Inner` we hold the only strong reference to, then that reference is
    /// dropped without going through `DiskCache::drop`, so the task is never
    /// aborted. Its next tick must upgrade to `None` and return on its own;
    /// awaiting the join handle must therefore complete rather than hang.
    ///
    /// FLIP (non-vacuity): change the sweep loop's `None => break` to
    /// `None => continue`. The task then never exits on a dead `Weak`, the
    /// `timeout` below elapses, and the `expect` fires.
    #[tokio::test]
    async fn sweeper_breaks_on_dead_weak_without_abort() {
        let tmp = TempDir::new().unwrap();
        // A short interval so the post-drop tick lands well inside the timeout.
        let limits = generous_limits().with_sweep_interval_ns(1_000_000); // 1 ms
        let inner = Arc::new(Inner {
            dir: tmp.path().to_path_buf(),
            limits,
            state: Mutex::new(S3Fifo::new(limits)),
            metrics: Arc::new(CacheMetrics::default()),
            tmp_counter: AtomicU64::new(0),
            instance_id: 0,
            clock: TestClock::new(1),
        });
        let handle = spawn_sweeper(&inner, &Handle::current());

        // Drop the sole strong reference. The task holds only a `Weak`, and we
        // deliberately do NOT abort it (there is no `DiskCache` to drop), so the
        // only way it can stop is the `None => break` path under test.
        drop(inner);

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("sweeper must exit on its own once its Weak is dead, not hang")
            .expect("the sweeper task must return cleanly, not panic");
    }

    /// Constructing a `DiskCache` off a Tokio runtime must
    /// fail loudly rather than silently skip the background sweep. Before this
    /// change the spawn was best-effort (`Handle::try_current().ok()?`), so an
    /// off-runtime construction returned a cache with no sweeper and, once the
    /// tier is wired, would leave the max-age residue bound unenforced for idle
    /// entries with no visible signal. This test is deliberately a plain
    /// `#[test]` (no `#[tokio::test]`) so the construction runs off every
    /// runtime; it must panic, as `tokio::spawn` itself does off-runtime.
    #[test]
    fn off_runtime_construction_fails_loudly() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        let limits = generous_limits();
        let result = std::panic::catch_unwind(|| {
            // Panics inside `new_with_clock`'s `Handle::current()`; the returned
            // cache is never bound, so no sweeper is silently skipped.
            let _ = DiskCache::new(dir, limits);
        });
        assert!(
            result.is_err(),
            "off-runtime construction must panic loudly, not return a sweeperless cache"
        );
    }

    /// Counts entry files under a namespaced root: every regular file under a
    /// two-hex shard subdirectory. Used by the #671 isolation tests to prove a
    /// namespace's on-disk file count independently of another namespace under
    /// the same configured directory.
    fn count_entry_files(dir: &Path) -> usize {
        let mut n = 0;
        let Ok(shards) = fs::read_dir(dir) else {
            return 0;
        };
        for shard in shards.flatten() {
            if shard.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                n += fs::read_dir(shard.path())
                    .map(|it| it.flatten().count())
                    .unwrap_or(0);
            }
        }
        n
    }

    /// Total entry files across every namespace subdirectory under `base`
    /// (`base/<namespace>/<shard>/<file>`). Distinct from summing
    /// [`count_entry_files`] on two known roots: it counts whatever is actually
    /// on disk, so if two instances collapsed onto one shared namespace the
    /// physical file total it returns is their union, not 2x a per-namespace
    /// count -- which is what makes it a decisive cross-eviction signal.
    fn count_all_entry_files(base: &Path) -> usize {
        let mut n = 0;
        let Ok(namespaces) = fs::read_dir(base) else {
            return 0;
        };
        for ns in namespaces.flatten() {
            if ns.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                n += count_entry_files(&ns.path());
            }
        }
        n
    }

    /// #671: two `DiskCache` instances over one directory with distinct
    /// namespace labels are fully isolated. Each enforces its own byte/entry
    /// budget over its own `dir/<namespace>` subtree, and neither counts nor
    /// evicts the other's files even when both hold the identical keys. All
    /// figures are pinned exactly.
    ///
    /// FLIP (non-vacuity): build both instances with the SAME label (e.g.
    /// `"store"` for both). Two changes fail immediately: `store.dir()` and
    /// `catalog.dir()` become equal (the `assert_ne` fires), and the
    /// base-wide `count_all_entry_files(base) == 8` collapses to 4, because the
    /// two instances now share one `dir/store` file set and their identical keys
    /// map to one file rather than two -- the cross-eviction this test forbids.
    #[tokio::test]
    async fn distinct_namespaces_over_one_dir_are_isolated() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_path_buf();
        let entry = 1024u64;
        // 4 entries / 4096 bytes: inserting 10 distinct once-touched keys forces
        // eviction down to exactly 4 resident (4 * 1024 == 4096).
        let limits = CacheLimits::new(entry * 4, 4, entry * 4);

        let store = DiskCache::new_in_namespace(base.clone(), "store", limits);
        let catalog = DiskCache::new_in_namespace(base.clone(), "catalog", limits);
        assert_ne!(
            store.dir(),
            catalog.dir(),
            "distinct labels, distinct roots"
        );

        // Identical keys in both instances: with distinct namespaces they land
        // in separate files (dir/store/... vs dir/catalog/...) and never collide.
        for i in 0..10u64 {
            let key = test_key_with_len(i, entry);
            store.insert(key, &vec![0xAAu8; entry as usize]);
            catalog.insert(key, &vec![0xBBu8; entry as usize]);
        }

        // Each stays within its own budget, pinned exactly.
        assert_eq!(store.len(), 4);
        assert_eq!(store.total_bytes(), 4096);
        assert_eq!(catalog.len(), 4);
        assert_eq!(catalog.total_bytes(), 4096);

        // Neither evicts the other's files: exactly 4 files under each
        // namespace, and 8 physical files across the shared directory. The
        // base-wide count is the decisive cross-eviction guard -- a shared
        // namespace would collapse it to 4.
        assert_eq!(count_entry_files(store.dir()), 4);
        assert_eq!(count_entry_files(catalog.dir()), 4);
        assert_eq!(count_all_entry_files(&base), 8);

        // The catalog's bytes are intact through its own reads (the store's
        // eviction never touched them): a surviving catalog key still serves
        // 0xBB bytes, not the store's 0xAA.
        let survivor = (0..10u64)
            .map(|i| test_key_with_len(i, entry))
            .find(|k| catalog.get(k).is_some())
            .expect("the catalog retains 4 of its own entries");
        assert_eq!(
            catalog.get(&survivor).as_deref(),
            Some(vec![0xBBu8; entry as usize].as_slice()),
            "the catalog serves its own bytes, unperturbed by the store's eviction"
        );
    }

    /// #671: a restart re-seeds each namespace from ONLY its own subtree. Two
    /// instances are populated with different entry counts, dropped, and
    /// reopened; each reopened instance's `len()` reflects only its own
    /// namespace, never the union.
    ///
    /// FLIP (non-vacuity): reopen the store instance under the `"catalog"` label
    /// (or a shared one). Its scan then reads the other subtree and `len()`
    /// becomes 3, not 2 -- proving the scan is namespace-scoped.
    #[tokio::test]
    async fn restart_reseeds_each_namespace_independently() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_path_buf();
        {
            let store = DiskCache::new_in_namespace(base.clone(), "store", generous_limits());
            store.insert(test_key_with_len(1, 10), &[1u8; 10]);
            store.insert(test_key_with_len(2, 10), &[2u8; 10]);
            let catalog = DiskCache::new_in_namespace(base.clone(), "catalog", generous_limits());
            catalog.insert(test_key_with_len(1, 20), &[3u8; 20]);
            catalog.insert(test_key_with_len(2, 20), &[4u8; 20]);
            catalog.insert(test_key_with_len(3, 20), &[5u8; 20]);
        }

        let store = DiskCache::new_in_namespace(base.clone(), "store", generous_limits());
        assert_eq!(store.len(), 2, "store re-seeds from only its own namespace");
        assert_eq!(store.total_bytes(), 20);

        let catalog = DiskCache::new_in_namespace(base.clone(), "catalog", generous_limits());
        assert_eq!(
            catalog.len(),
            3,
            "catalog re-seeds from only its own namespace"
        );
        assert_eq!(catalog.total_bytes(), 60);
    }

    /// #671: entries left in the directory ROOT by a build from before
    /// namespacing (the old `dir/<shard>/<file>` layout, no namespace
    /// subdirectory) are claimed by NO namespace. `scan_existing` reads only
    /// `dir/<namespace>`, so neither a "store" nor a "catalog" instance seeds
    /// such a file into its budget or deletes it; it persists untouched until an
    /// operator (or a future full-directory sweep) removes it.
    ///
    /// FLIP (non-vacuity): construct an instance with an empty label
    /// (`new_in_namespace(base, "", ..)`), whose namespaced root collapses back
    /// to `base`. Its `scan_existing` then traverses the root shard, seeds the
    /// warm entry (`len()` becomes 1), and the "counted in neither / not
    /// deleted" assertions fail.
    #[tokio::test]
    async fn warm_root_entries_are_claimed_by_no_namespace() {
        let tmp = TempDir::new().unwrap();
        let base = tmp.path().to_path_buf();

        // A well-formed entry at the pre-namespacing root layout: base/<shard>/
        // <file>, with no namespace subdirectory in the path.
        let key = test_key_with_len(1, 64);
        let payload = vec![7u8; 64];
        let warm_path = path_for(&base, &key);
        fs::create_dir_all(warm_path.parent().unwrap()).unwrap();
        let clock = TestClock::new(1_000_000);
        let mut buf = Vec::new();
        buf.extend_from_slice(&encode_header(&key, &payload, 1_000_000)); // within max-age at now
        buf.extend_from_slice(&payload);
        fs::write(&warm_path, &buf).unwrap();

        let store = DiskCache::new_in_namespace_with_clock(
            base.clone(),
            "store",
            generous_limits(),
            clock.clone(),
        );
        let catalog = DiskCache::new_in_namespace_with_clock(
            base.clone(),
            "catalog",
            generous_limits(),
            clock,
        );

        // Counted in neither budget: each scanned only its own namespace.
        assert_eq!(store.len(), 0);
        assert_eq!(store.total_bytes(), 0);
        assert_eq!(catalog.len(), 0);
        assert_eq!(catalog.total_bytes(), 0);

        // Not deleted by either scan: the warm file sits at the root, outside
        // both `dir/store` and `dir/catalog`, so no per-namespace scan reaches
        // it.
        assert!(
            warm_path.exists(),
            "a pre-namespacing root entry must survive both instances' scans untouched"
        );

        // And it is a genuine miss through each instance, which looks only under
        // its own namespace where this key has no file.
        assert!(store.get(&key).is_none());
        assert!(catalog.get(&key).is_none());
    }

    /// The namespace must be a single non-empty path segment, enforced at
    /// runtime rather than by convention. An empty label is the dangerous case:
    /// `Path::join("")` returns the base directory unchanged, so without this
    /// guard two instances constructed with `""` would share one root again and
    /// evict each other's entries, which is exactly the bug namespacing fixes,
    /// reintroduced silently. Removing the assert makes this test pass a value
    /// that collapses the root.
    #[tokio::test]
    #[should_panic(expected = "exactly one ordinary path component")]
    async fn an_empty_namespace_is_refused_rather_than_collapsing_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _ = DiskCache::new_in_namespace(dir.path().to_path_buf(), "", generous_limits());
    }

    /// A label with a separator escapes the intended one-segment layout, and a
    /// leading slash makes `Path::join` discard the base directory entirely.
    #[tokio::test]
    #[should_panic(expected = "exactly one ordinary path component")]
    async fn a_multi_segment_namespace_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let _ = DiskCache::new_in_namespace(dir.path().to_path_buf(), "a/b", generous_limits());
    }
}

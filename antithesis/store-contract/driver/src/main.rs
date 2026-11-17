//! Antithesis driver for Ravel's object-store contract
//! (docs/object-store-contract.md, formal/tla/common): the same operations
//! the TLA+ model `Store` names, issued through `S3Store` against RustFS,
//! with every result judged against what the driver itself wrote.
//!
//! The driver is a hegel concurrent state machine. Each rule runs one action
//! from `model.rs` under a fresh key prefix and asserts the property that
//! action checks. See antithesis/store-contract/README.md for the
//! properties and the mutants.

#[macro_use]
mod mutant;
mod model;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use antithesis_sdk::{
    assert_always, assert_always_or_unreachable, assert_reachable, assert_sometimes,
};
use bytes::Bytes;
use hegel::TestCase;
use hegel::generators as gs;
use hegel::stateful::machine;
use ravel_object_store::s3::{S3AuthMode, S3Config, S3HttpConfig, S3Store};
use ravel_object_store::{MULTIPART_MIN_PART_SIZE, ObjectStoreBackend};
use serde_json::json;

use antithesis_instrumentation as _;

use crate::model::Judged;
use crate::mutant::{BrokenStore, Mutant};

struct Config {
    s3: S3Config,
    page_size: usize,
    hurried_timeout: Duration,
    max_actions: Option<u64>,
    mutant: Option<Mutant>,
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.is_empty())
}

fn env_num(name: &str, default: u64) -> Result<u64, String> {
    match env(name) {
        None => Ok(default),
        Some(v) => v
            .parse()
            .map_err(|e| format!("{name}={v} is not a number: {e}")),
    }
}

impl Config {
    fn from_env() -> Result<Config, String> {
        let need = |name: &str| env(name).ok_or_else(|| format!("{name} is not set"));
        let mutant = Mutant::select(std::env::var("RAVEL_ANTITHESIS_MUTANT").ok().as_deref())?;
        Ok(Config {
            s3: S3Config {
                bucket: need("RAVEL_S3_BUCKET")?,
                region: env("RAVEL_S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
                endpoint: Some(need("RAVEL_S3_ENDPOINT")?),
                access_key_id: need("RAVEL_S3_ACCESS_KEY")?,
                secret_access_key: need("RAVEL_S3_SECRET_KEY")?,
                allow_http: env("RAVEL_S3_ALLOW_HTTP").is_some_and(|v| v == "true" || v == "1"),
                force_path_style: true,
                kms_key_id: None,
                session_token: None,
                credentials_file: None,
                auth: S3AuthMode::Static,
                instance_metadata_endpoint: None,
            },
            page_size: usize::try_from(env_num("STORE_LIST_PAGE_SIZE", 2)?.max(1))
                .map_err(|e| e.to_string())?,
            hurried_timeout: Duration::from_millis(env_num("STORE_HURRIED_TIMEOUT_MS", 5)?),
            max_actions: env("WORKLOAD_MAX_ACTIONS")
                .map(|v| {
                    v.parse()
                        .map_err(|e| format!("WORKLOAD_MAX_ACTIONS={v}: {e}"))
                })
                .transpose()?,
            mutant,
        })
    }
}

struct Shared {
    rt: tokio::runtime::Runtime,
    store: Arc<BrokenStore>,
    /// The same bucket behind a request timeout short enough that some
    /// writes fail mid-flight.
    hurried: BrokenStore,
    page_size: usize,
    run: String,
    next: AtomicU64,
    max_actions: Option<u64>,
}

impl Shared {
    /// A fresh key prefix for one action, or `None` once the action cap is
    /// reached (the local loop's way to end a timeline).
    fn base(&self, action: &str) -> String {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        if let Some(max) = self.max_actions
            && id >= max
        {
            println!("driver: {max} actions run, ending the timeline");
            std::process::exit(0);
        }
        format!("sc/{}/{action}/{id}", self.run)
    }
}

/// Print an inconclusive action: faults left it nothing definite to judge.
fn inconclusive(base: &str, j: Judged) -> Option<model::Verdict> {
    match j {
        Ok(v) => Some(v),
        Err(e) => {
            println!("{base}: inconclusive: {e}");
            None
        }
    }
}

struct Workload(Arc<Shared>);

#[hegel::concurrent_state_machine]
impl Workload {
    #[rule(groups = ["main"], weight = 3)]
    fn create_race(&self, tc: TestCase) {
        let s = &self.0;
        let n = tc.draw(gs::integers::<usize>().min_value(2).max_value(4));
        let base = s.base("create");
        let Some(v) = inconclusive(&base, s.rt.block_on(model::create_race(&s.store, &base, n)))
        else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a create-if-absent key has exactly one winner",
            &v.details
        );
        if v.contended {
            assert_reachable!("store: a create race had a loser", &v.details);
        }
    }

    #[rule(groups = ["main"], weight = 3)]
    fn cas(&self, _tc: TestCase) {
        let s = &self.0;
        let base = s.base("cas");
        let Some(v) = inconclusive(&base, s.rt.block_on(model::cas(&s.store, &base))) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a CAS outcome matches its effect",
            &v.details
        );
        if v.stale_refused {
            assert_reachable!("store: a stale CAS was refused", &v.details);
        }
    }

    #[rule(groups = ["main"], weight = 2)]
    fn read_after_write(&self, tc: TestCase) {
        let s = &self.0;
        let writes = tc.draw(gs::integers::<usize>().min_value(1).max_value(3));
        let base = s.base("raw");
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::read_after_write(&s.store, &base, writes)),
        ) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a successful write is immediately readable with its version",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 2)]
    fn delete(&self, _tc: TestCase) {
        let s = &self.0;
        let base = s.base("delete");
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::delete_idempotent(&s.store, &base)),
        ) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a delete is idempotent and touches only its key",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 2)]
    fn versions(&self, _tc: TestCase) {
        let s = &self.0;
        let base = s.base("versions");
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::versions_never_reused(&s.store, &base)),
        ) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a version is never reused after a delete",
            &v.details
        );
        assert_always!(
            v.same_bytes_ok,
            "store: a CAS holding a pre-delete version fails after a same-bytes recreate",
            &v.details
        );
        // On RustFS the ETag is content-derived, so the recreate reuses the
        // pre-delete version; record that reuse rather than requiring the
        // version to differ, which the TLA+ global counter would but S3Store
        // as Ravel uses it does not (README "Versions on S3").
        assert_sometimes!(
            v.same_bytes_reused,
            "store: a same-bytes recreate reuses the version",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 1)]
    fn multipart(&self, tc: TestCase) {
        let s = &self.0;
        let large = tc.draw(gs::weighted_booleans(0.25));
        let complete = tc.draw(gs::weighted_booleans(0.75));
        let base = s.base("multipart");
        let mut parts = Vec::new();
        if large {
            let mut first = format!("{base}/p0/").into_bytes();
            first.resize(MULTIPART_MIN_PART_SIZE, b'.');
            parts.push(Bytes::from(first));
        }
        parts.push(Bytes::from(format!("{base}/last")));
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::multipart(&s.store, &base, &parts, complete)),
        ) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a multipart upload is invisible until complete",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 2)]
    fn list(&self, tc: TestCase) {
        let s = &self.0;
        let keys = tc.draw(gs::integers::<usize>().min_value(3).max_value(9));
        let base = s.base("list");
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::list_paged(&s.store, &base, keys, s.page_size)),
        ) else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a paged list ends complete within its page bound",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 2)]
    fn lost_response(&self, _tc: TestCase) {
        let s = &self.0;
        let base = s.base("lost");
        let Some(v) = inconclusive(&base, s.rt.block_on(model::lost_response(&s.store, &base)))
        else {
            return;
        };
        assert_always!(
            v.holds,
            "store: a lost-response overwrite took effect",
            &v.details
        );
    }

    #[rule(groups = ["main"], weight = 2)]
    fn transient(&self, tc: TestCase) {
        let s = &self.0;
        let len = tc.draw(gs::integers::<usize>().min_value(64).max_value(256 * 1024));
        let base = s.base("transient");
        let Some(v) = inconclusive(
            &base,
            s.rt.block_on(model::transient(&s.hurried, &s.store, &base, len)),
        ) else {
            return;
        };
        if v.failed {
            assert_reachable!("store: a write failed with a retryable error", &v.details);
            assert_always_or_unreachable!(
                v.holds,
                "store: a failed write leaves nothing or the whole write",
                &v.details
            );
        }
        if v.failed_applied {
            assert_reachable!("store: a failed write was later found applied", &v.details);
        }
    }
}

/// Wait, with no deadline of its own, until `check` passes, printing the
/// reason whenever it changes.
fn wait_for(what: &str, mut check: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    loop {
        match check() {
            Ok(()) => {
                println!("startup: {what}");
                return;
            }
            Err(e) => {
                if e != last {
                    println!("startup: waiting until {what}: {e}");
                    last = e;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn build(cfg: &Config, rt: &tokio::runtime::Runtime) -> Result<(BrokenStore, BrokenStore), String> {
    let _enter = rt.enter();
    let normal = S3Store::with_page_size(cfg.s3.clone(), cfg.page_size)
        .map_err(|e| format!("building the store: {e}"))?;
    let http = S3HttpConfig {
        request_timeout: cfg.hurried_timeout,
        ..S3HttpConfig::default()
    };
    let hurried = S3Store::with_http_config_and_page_size(cfg.s3.clone(), http, cfg.page_size)
        .map_err(|e| format!("building the hurried store: {e}"))?;
    Ok((
        BrokenStore::new(Arc::new(normal), cfg.mutant),
        BrokenStore::new(Arc::new(hurried), cfg.mutant),
    ))
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("driver: {e}");
            std::process::exit(2);
        }
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("driver: tokio runtime: {e}");
            std::process::exit(1);
        }
    };
    let (store, hurried) = match build(&cfg, &rt) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("driver: {e}");
            std::process::exit(1);
        }
    };
    let store = Arc::new(store);
    let mutant = cfg.mutant.map_or("none", Mutant::name);
    println!("startup: mutant {mutant}, list page size {}", cfg.page_size);

    wait_for("the bucket answers a listing", || {
        rt.block_on(store.list("sc/", None))
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let run = format!("{nanos:x}-{}", std::process::id());
    antithesis_sdk::lifecycle::setup_complete(
        &json!({ "bucket": cfg.s3.bucket, "mutant": mutant, "run": run }),
    );
    println!("startup: setup complete; waiting for /workload-active");
    while !Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("store: the workload starts", &json!({ "mutant": mutant }));

    let shared = Arc::new(Shared {
        rt,
        store,
        hurried,
        page_size: cfg.page_size,
        run,
        next: AtomicU64::new(0),
        max_actions: cfg.max_actions,
    });
    machine(Workload(shared))
        .steps(i64::MAX)
        .max_concurrency(4)
        .run_concurrent(tc);
}

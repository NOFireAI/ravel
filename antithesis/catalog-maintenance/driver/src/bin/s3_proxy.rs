//! The `s3proxy` container (README.md, "The forced races"): every
//! ravel-server container reaches rustfs through it, and the driver sets its
//! rules over the control port. It runs in its own container rather than in
//! the driver because the driver is excluded from faults, and routing the
//! servers' store traffic through it would exclude that traffic too.
//!
//! `S3_PROXY_LISTEN` (default `0.0.0.0:9000`), `S3_PROXY_CONTROL` (default
//! `0.0.0.0:9001`), `S3_PROXY_UPSTREAM` (default `rustfs:9000`) and
//! `RAVEL_S3_BUCKET` (default `ravel`).
//!
//! At startup it raises its soft open-file limit to the hard limit, as
//! ravel-server does, since container runtimes commonly leave the soft limit
//! at 1024 under a far higher hard limit. It refuses to start if the limit
//! in force is still below what its connection caps need
//! (`proxy::NOFILE_NEEDED`), and its startup line states the limit, how it
//! was reached and the cap.

use std::net::TcpListener;
use std::sync::Arc;

use antithesis_instrumentation as _;
use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};

use ravel_antithesis_catalog_driver::proxy::{self, Rules, Slots};

fn var(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn bind(addr: &str) -> TcpListener {
    TcpListener::bind(addr).unwrap_or_else(|e| {
        eprintln!("s3-proxy: binding {addr}: {e}");
        std::process::exit(1);
    })
}

/// Raise the soft open-file limit to the hard limit, then return the soft
/// limit in force and how it was reached.
fn raise_nofile() -> (Option<u64>, String) {
    let before = getrlimit(Resource::Nofile);
    let source = match proxy::raised_soft(before.current, before.maximum) {
        None => "inherited".to_string(),
        Some(target) => {
            let raised = Rlimit {
                current: Some(target),
                maximum: before.maximum,
            };
            match setrlimit(Resource::Nofile, raised) {
                Ok(()) => format!("raised from {}", render(before.current)),
                Err(e) => {
                    eprintln!("s3-proxy: raising the soft open-file limit to {target}: {e}");
                    "raise refused".to_string()
                }
            }
        }
    };
    (getrlimit(Resource::Nofile).current, source)
}

fn render(limit: Option<u64>) -> String {
    limit.map_or_else(|| "unlimited".to_string(), |n| n.to_string())
}

fn main() {
    let (nofile, nofile_source) = raise_nofile();
    if let Err(e) = proxy::check_nofile(nofile) {
        eprintln!("s3-proxy: refusing to start: {e}");
        std::process::exit(1);
    }
    let listen = var("S3_PROXY_LISTEN", "0.0.0.0:9000");
    let control = var("S3_PROXY_CONTROL", "0.0.0.0:9001");
    let upstream = var("S3_PROXY_UPSTREAM", "rustfs:9000");
    let bucket = var("RAVEL_S3_BUCKET", "ravel");
    let rules = Arc::new(Rules::default());
    let data = bind(&listen);
    let ctl = bind(&control);
    let r = Arc::clone(&rules);
    let control_slots = Slots::new(proxy::MAX_CONTROL_CONNECTIONS);
    if let Err(e) = std::thread::Builder::new()
        .name("s3-proxy-control-accept".to_string())
        .spawn(move || proxy::serve_control(ctl, r, control_slots, proxy::IO_TIMEOUT))
    {
        eprintln!("s3-proxy: starting the control port: {e}");
        std::process::exit(1);
    }
    println!(
        "s3-proxy: {listen} to {upstream} (bucket {bucket}), control on {control}, at most {} \
         connections, open-file limit {} ({nofile_source}, {} needed), {} s socket timeout",
        proxy::MAX_CONNECTIONS,
        render(nofile),
        proxy::NOFILE_NEEDED,
        proxy::IO_TIMEOUT.as_secs()
    );
    proxy::serve(
        data,
        upstream,
        bucket,
        rules,
        Slots::new(proxy::MAX_CONNECTIONS),
        proxy::IO_TIMEOUT,
    );
}

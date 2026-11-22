//! The `s3proxy` container (README.md, "The forced races"): every
//! ravel-server container reaches rustfs through it, and the driver sets its
//! rules over the control port. It runs in its own container rather than in
//! the driver because the driver is excluded from faults, and routing the
//! servers' store traffic through it would exclude that traffic too.
//!
//! `S3_PROXY_LISTEN` (default `0.0.0.0:9000`), `S3_PROXY_CONTROL` (default
//! `0.0.0.0:9001`), `S3_PROXY_UPSTREAM` (default `rustfs:9000`) and
//! `RAVEL_S3_BUCKET` (default `ravel`).

use std::net::TcpListener;
use std::sync::Arc;

use antithesis_instrumentation as _;

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

fn main() {
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
         connections, {} s socket timeout",
        proxy::MAX_CONNECTIONS,
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

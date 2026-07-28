//! Spawns and tears down the pinned Prometheus binary with a fresh tsdb
//! directory and `--web.enable-remote-write-receiver` (ADR-0021 decision 4,
//! docs/promql-evaluator-plan.md section 5.1).
//!
//! Uses `std::process::Command` (blocking spawn/kill), not tokio's async
//! process API: the workspace's shared `tokio` dependency does not enable
//! the `process` feature, and adding it would touch the root `Cargo.toml`,
//! out of this ticket's scope (crate-local changes only). Readiness polling
//! is async (`PrometheusClient::wait_ready`); only the spawn and kill calls
//! are blocking, and both are quick, non-blocking-runtime-hazard syscalls.

use std::io;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use tempfile::TempDir;

#[derive(Debug, thiserror::Error)]
pub enum PrometheusProcessError {
    #[error("spawning prometheus binary at {path}: {source}")]
    Spawn { path: PathBuf, source: io::Error },
    #[error("allocating a free tcp port: {0}")]
    Port(io::Error),
    #[error("creating scratch tsdb directory: {0}")]
    TempDir(io::Error),
    #[error("writing scratch prometheus config: {0}")]
    WriteConfig(io::Error),
}

/// A running pinned Prometheus instance. Killed and its scratch directory
/// removed on drop.
pub struct PrometheusProcess {
    child: Child,
    _data_dir: TempDir,
    pub base_url: String,
}

impl PrometheusProcess {
    /// Spawns `bin_path` with a fresh tsdb directory under a new temp dir,
    /// remote-write receiving enabled, and an empty scrape config (this
    /// harness only ever pushes data via remote write, never scrapes).
    pub fn spawn(bin_path: &Path) -> Result<Self, PrometheusProcessError> {
        let data_dir = TempDir::new().map_err(PrometheusProcessError::TempDir)?;
        let config_path = data_dir.path().join("prometheus.yml");
        std::fs::write(
            &config_path,
            "global:\n  scrape_interval: 15s\nscrape_configs: []\n",
        )
        .map_err(PrometheusProcessError::WriteConfig)?;
        let tsdb_path = data_dir.path().join("tsdb");
        std::fs::create_dir_all(&tsdb_path).map_err(PrometheusProcessError::TempDir)?;

        let port = free_tcp_port().map_err(PrometheusProcessError::Port)?;
        let listen_address = format!("127.0.0.1:{port}");

        let child = Command::new(bin_path)
            .arg(format!("--config.file={}", config_path.display()))
            .arg(format!("--storage.tsdb.path={}", tsdb_path.display()))
            .arg(format!("--web.listen-address={listen_address}"))
            .arg("--web.enable-remote-write-receiver")
            .arg("--log.level=warn")
            .arg("--storage.tsdb.min-block-duration=2h")
            .arg("--storage.tsdb.retention.time=1h")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|source| PrometheusProcessError::Spawn {
                path: bin_path.to_path_buf(),
                source,
            })?;

        Ok(PrometheusProcess {
            child,
            _data_dir: data_dir,
            base_url: format!("http://{listen_address}"),
        })
    }
}

impl Drop for PrometheusProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Binds an ephemeral port to find one that is currently free, then drops
/// the listener so Prometheus can bind it. Inherently racy against another
/// process grabbing the same port between the bind and the spawn; acceptable
/// for a local differential-test harness with no concurrent instances.
fn free_tcp_port() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    listener.local_addr().map(|addr| addr.port())
}

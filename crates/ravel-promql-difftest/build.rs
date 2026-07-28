//! Compiles the vendored RW1 proto (proto/prometheus/prompb, pinned
//! Prometheus v3.13.1, same tag as crates/ravel-remote-write's vendoring)
//! with protox; no protoc or network needed.
//!
//! This crate vendors its own copy of the compile step rather than
//! depending on ravel-remote-write's generated types (docs/promql-
//! evaluator-plan.md section 5.1: "the remote-write proto is vendored
//! crate-locally"). ravel-remote-write is a production ingest-decode
//! surface; this crate only ever encodes a `WriteRequest` to push fixtures
//! into the differential harness's pinned Prometheus instance, so it is
//! kept independent. Only `types.proto` and `remote.proto` are compiled:
//! this harness always speaks Remote Write 1.0 (matching Prometheus'
//! `--web.enable-remote-write-receiver` default protocol), so the RW2
//! symbol-table proto is not needed here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto/prometheus/prompb");
    let types_file = root.join("types.proto");
    let remote_file = root.join("remote.proto");
    println!("cargo:rerun-if-changed={}", types_file.display());
    println!("cargo:rerun-if-changed={}", remote_file.display());
    let descriptors = protox::compile([&types_file, &remote_file], [&root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}

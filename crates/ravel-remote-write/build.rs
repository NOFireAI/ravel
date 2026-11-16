//! Compiles the vendored RW1 and RW2 protos (proto/prometheus/prompb,
//! pinned Prometheus v3.13.1) with protox; no protoc or network needed.
//!
//! RW1 (`prometheus.WriteRequest`, package `prometheus`) and RW2
//! (`io.prometheus.write.v2.Request`, package `io.prometheus.write.v2`) are
//! independent message families with no shared imports, so one protox
//! compile pass over both produces two separate generated files.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto/prometheus/prompb");
    let types_file = root.join("types.proto");
    let remote_file = root.join("remote.proto");
    let v2_types_file = root.join("io/prometheus/write/v2/types.proto");
    println!("cargo:rerun-if-changed={}", types_file.display());
    println!("cargo:rerun-if-changed={}", remote_file.display());
    println!("cargo:rerun-if-changed={}", v2_types_file.display());
    let descriptors = protox::compile([&types_file, &remote_file, &v2_types_file], [&root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}

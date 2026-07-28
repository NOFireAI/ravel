//! Compiles the vendored RW1 protos (proto/prometheus/prompb, pinned
//! Prometheus v3.13.1) with protox; no protoc or network needed.
//!
//! proto/prometheus/prompb/io/prometheus/write/v2/types.proto (the RW2
//! message family) is vendored alongside these for the phase-2 decoder
//! (docs/ingest-breadth-plan.md ticket A2) but is not compiled here: this
//! crate only decodes RW1 so far.

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

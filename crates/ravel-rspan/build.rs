//! Compiles `proto/ravel/rspan.proto` with protox (pure Rust; no protoc
//! needed), the same build path `ravel-proto` uses. The schema lives in the
//! shared repo-root `proto/ravel/` directory alongside every other Ravel
//! on-object schema (segment, commit, catalog, logseg); it is still compiled
//! here into this crate's private `pb` module rather than by `ravel-proto`, so
//! the generated span-footer types stay crate-local. RSPAN's footer is a
//! separate message from RLOG's `ravel.logseg.v1.LogFooter` (ADR-0041): a span
//! object has no stream identity and a different summary shape, so extending
//! the frozen RLOG footer would add span-only fields to a logs contract.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let file = root.join("ravel/rspan.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    let descriptors = protox::compile([&file], [&root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}

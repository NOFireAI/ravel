//! Compiles this crate's own `proto/rspan.proto` with protox (pure Rust; no
//! protoc needed), the same build path `ravel-proto` uses. RSPAN's footer is a
//! separate message from RLOG's `ravel.logseg.v1.LogFooter` (ADR-0041): a span
//! object has no stream identity and a different summary shape, so extending
//! the frozen RLOG footer would add span-only fields to a logs contract. Keeping
//! the proto crate-local also keeps the whole span format inside this crate,
//! touching no other crate's frozen schemas.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    let file = root.join("rspan.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    let descriptors = protox::compile([&file], [&root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}

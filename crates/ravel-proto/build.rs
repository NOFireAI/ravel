//! Compiles proto/ravel/*.proto with protox (pure Rust; no protoc needed).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = [
        root.join("ravel/segment.proto"),
        root.join("ravel/commit.proto"),
        root.join("ravel/catalog.proto"),
        root.join("ravel/logseg.proto"),
    ];
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    let descriptors = protox::compile(&files, [&root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}

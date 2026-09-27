//! Compiles proto/ravel/*.proto with protox (pure Rust; no protoc needed).

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = [
        root.join("ravel/segment.proto"),
        root.join("ravel/commit.proto"),
        root.join("ravel/catalog.proto"),
        root.join("ravel/logseg.proto"),
        root.join("ravel/sys.proto"),
        root.join("ravel/queryfrag.proto"),
        root.join("ravel/parquet_table.proto"),
    ];
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    let descriptors = protox::compile(&files, [&root])?;
    // BTreeMap for the Parquet table manifest's `options`, so encoding a
    // manifest is deterministic.
    prost_build::Config::new()
        .btree_map([".ravel.parquet_table.v1"])
        .compile_fds(descriptors)?;
    Ok(())
}

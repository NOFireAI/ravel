//! Generates the ADR-0071 `SeriesFetch` gRPC service (client and server) from
//! `proto/queryfrag_service.proto`, reusing the frozen message types from
//! `ravel-proto` rather than regenerating them.
//!
//! Compiled with protox (pure Rust; no protoc or network needed), matching
//! ravel-proto's and ravel-otap's build scripts. The `extern_path` mapping
//! points every `ravel.queryfrag.v1` message the service references at
//! `ravel_proto::queryfrag::v1`, so this build emits only the service stubs
//! (the two `SeriesFetch` client/server modules), never a second copy of the
//! frozen messages.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_proto = manifest.join("../../proto");
    let crate_proto = manifest.join("proto");
    let service = crate_proto.join("queryfrag_service.proto");

    println!("cargo:rerun-if-changed={}", service.display());
    println!(
        "cargo:rerun-if-changed={}",
        workspace_proto.join("ravel/queryfrag.proto").display()
    );

    // Include roots: the crate's own proto dir (the service file) and the
    // workspace proto root (so `import "ravel/queryfrag.proto"` resolves).
    let descriptors = protox::compile([&service], [&crate_proto, &workspace_proto])?;

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        // Reuse the frozen, already-generated message types from ravel-proto
        // instead of generating a second copy in this crate's OUT_DIR.
        .extern_path(".ravel.queryfrag.v1", "::ravel_proto::queryfrag::v1")
        .compile_fds(descriptors)?;
    Ok(())
}

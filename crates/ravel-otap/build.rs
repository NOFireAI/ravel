//! Compiles the vendored OTAP protos (proto/otel-arrow, pinned v0.50.0)
//! with protox; no protoc or network needed.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto/otel-arrow");
    let file = root.join("opentelemetry/proto/experimental/arrow/v1/arrow_service.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    let descriptors = protox::compile([&file], [&root])?;
    tonic_prost_build::configure()
        .build_client(true) // test-side encoder drives the service in-process
        .build_server(true)
        // Decode ArrowPayload.record as bytes::Bytes so tonic's frame bytes
        // are sub-sliced with refcounting instead of copied into a Vec<u8>.
        .bytes("opentelemetry.proto.experimental.arrow.v1.ArrowPayload.record")
        .compile_fds(descriptors)?;
    Ok(())
}

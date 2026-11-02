//! Compiles the vendored, bytes-retyped OTLP logs protos (proto/retyped, see
//! proto/README.md) with protox; no protoc or network needed. Message types
//! only: no tonic service code is generated here.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto/retyped");
    let files = [
        root.join("opentelemetry/proto/collector/logs/v1/logs_service.proto"),
        root.join("opentelemetry/proto/logs/v1/logs.proto"),
        root.join("opentelemetry/proto/common/v1/common.proto"),
        root.join("opentelemetry/proto/resource/v1/resource.proto"),
    ];
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }
    let descriptors = protox::compile(&files, [&root])?;
    // Every bytes-typed field (the six retyped fields, plus LogRecord's
    // already-bytes trace_id/span_id) decodes as bytes::Bytes: a sub-slice of
    // the request body, refcounted rather than copied.
    prost_build::Config::new()
        .bytes(["."])
        .compile_fds(descriptors)?;
    Ok(())
}

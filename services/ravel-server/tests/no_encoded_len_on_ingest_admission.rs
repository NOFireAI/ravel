//! Guard: no gRPC ingest admission call site may charge
//! byte-rate on `Message::encoded_len()` (an `O(message)` walk of the
//! decoded protobuf tree) again. Charging must come from `WireByteCountLayer`
//! (`src/wire_byte_count.rs`) reading transport-derived wire bytes instead.
//!
//! A plain substring grep would also flag this guard's own module docs and
//! the doc comments in the files it scans, which mention `encoded_len` by
//! name to explain what they replaced -- so this only flags a line with an
//! actual `encoded_len(` call expression that is not itself a `//` comment.

use std::fs;

const INGEST_ADMISSION_PATH_FILES: &[&str] = &[
    "src/otlp_grpc.rs",
    "src/otlp_grpc_logs.rs",
    "src/otlp_grpc_traces.rs",
    "src/otap_grpc.rs",
];

#[test]
fn ingest_admission_path_never_calls_encoded_len() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let mut offending = Vec::new();
    for relative in INGEST_ADMISSION_PATH_FILES {
        let path = format!("{manifest_dir}/{relative}");
        let contents = fs::read_to_string(&path).unwrap_or_else(|err| panic!("read {path}: {err}"));
        for (line_no, line) in contents.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if line.contains("encoded_len(") {
                offending.push(format!("{relative}:{}: {}", line_no + 1, line.trim()));
            }
        }
    }
    assert!(
        offending.is_empty(),
        "ingest admission path must charge wire bytes via WireByteCountLayer, not \
         Message::encoded_len(); found call(s): {offending:?}"
    );
}

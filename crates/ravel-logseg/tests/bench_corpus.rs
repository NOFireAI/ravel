//! Proves the synthetic corpus the logseg benches build (`benches/support.rs`)
//! actually round-trips through `RlogWriter` -> bytes -> `RlogReader::scan`,
//! so the benches measure real encode/decode work rather than garbage input
//! (issue #267). Not timed: a fast correctness check, run under `cargo test`.
#![allow(clippy::expect_used)]

use ravel_logseg::{Predicate, RlogReader, RlogWriter};

#[path = "../benches/common/mod.rs"]
mod support;
use support::{bench_config, bench_identity, build_corpus};

#[test]
fn corpus_roundtrips() {
    // Multiple streams, mixed attrs (support::make_record gives every record
    // int/float/bool/string attrs plus an occasional planted "needle" token).
    let corpus = build_corpus(5, 40);
    assert_eq!(corpus.len(), 200);

    let cfg = bench_config();
    let mut w = RlogWriter::new(cfg, bench_identity());
    for r in &corpus {
        w.push(r.clone()).expect("push");
    }
    let object = w.finish().expect("finish");

    let reader = RlogReader::new(&object, &cfg).expect("open");
    let (mut scanned, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
    assert_eq!(scanned.len(), corpus.len());

    let mut written = corpus;
    let sort_key = |r: &ravel_logseg::LogRecord| (r.stream_id, r.ts_ns);
    written.sort_by_key(sort_key);
    scanned.sort_by_key(sort_key);
    // The reader rebuilds `attrs` from FIELD_DIR order (sorted by name, type),
    // not the writer's insertion order, so normalize both sides by attr name
    // before comparing: the writer preserves no dynamic-attribute ordering
    // contract, only the set of (name, value) pairs.
    for r in written.iter_mut().chain(scanned.iter_mut()) {
        r.attrs.sort_by(|a, b| a.0.cmp(&b.0));
    }
    assert_eq!(
        written, scanned,
        "scan must recover exactly what was written"
    );
}

# Ravel Technical Due Diligence: Architecture, Correctness, Security and Production Readiness

> Status: DRAFT IN PROGRESS. Sections fill incrementally; a section marked
> NOT YET WRITTEN has not been completed. A section marked NOT ASSESSED could
> not be examined for the stated reason.

## 1. Executive verdict

NOT YET WRITTEN.

## 2. Review provenance and methodology

### Frozen subject

- Repository: Ravel (this checkout), a multi-tenant telemetry database.
- Branch/dispatch: dispatched from `main`.
- Frozen commit SHA: `527a16db2e4d47b2924e4de4a4db32d7583fda33`.
- Commit timestamp: `2026-08-22T22:53:40+03:00`.
- Tags/releases visible: none (`git tag` empty). No published version state.
- The frozen SHA, not moving `main`, is the subject of this entire report.

### Environment

- Toolchain: rustc 1.97.1 (8bab26f4f 2026-07-14), cargo 1.97.1.
- Host: 4 cores, 7.9 GiB RAM, 404 GiB free disk.
- cargo builds/tests run with `--jobs 2` (8 GB host: default parallelism gets
  the linker OOM-killed).
- Absent tooling: cargo-nextest, cargo-deny, cargo-audit. Docker present but
  daemon unreachable (permission denied).

### Scope of what ran vs what did not

- Static analysis, code reading, `cargo metadata`, `cargo tree`, `cargo fmt
  --check`, `cargo clippy`, and targeted `cargo test -p <crate>` on the
  highest-risk crates: attempted (results in section 21 and the evidence
  appendix).
- MinIO/S3 integration, kind/Kubernetes, container builds requiring the Docker
  daemon: NOT ASSESSED (environmental: no Docker access on host).

### Methodology

Twelve independent specialist investigations (agents A through L) each read
code and tests directly and produced an evidence memo under
`due-diligence/memos/`. A second adversarial pass (`due-diligence/rebuttals.md`)
challenged each Critical/High finding. Every command and its real exit code is
logged in `due-diligence/evidence/commands.md`. Evidence labels follow the
charter taxonomy (VERIFIED, STRONGLY SUPPORTED, IMPLEMENTED/WEAKLY VERIFIED,
DOCUMENTED CLAIM, CONTRADICTED, UNKNOWN, NOT IMPLEMENTED, NOT ASSESSED).

## 3. Architecture in one page

NOT YET WRITTEN.

## 4. The strongest parts of the design

NOT YET WRITTEN.

## 5. Top findings and blockers

NOT YET WRITTEN.

## 6. Production-readiness scorecard

NOT YET WRITTEN.

## 7. Claim Verification Matrix

NOT YET WRITTEN.

## 8. Consistency and durability analysis

NOT YET WRITTEN.

## 9. Catalog, snapshot and commit-token correctness

NOT YET WRITTEN.

## 10. Compaction, retention and GC safety

NOT YET WRITTEN.

## 11. Distributed query and federation

NOT YET WRITTEN.

## 12. Data formats and upgrade compatibility

NOT YET WRITTEN.

## 13. PromQL, SQL, query correctness

NOT YET WRITTEN.

## 14. Rust engineering assessment

NOT YET WRITTEN.

## 15. Security and multi-tenancy threat model

NOT YET WRITTEN.

## 16. Observability-product assessment

NOT YET WRITTEN.

## 17. SRE, operations, Kubernetes review

NOT YET WRITTEN.

## 18. Disaster-recovery assessment

NOT YET WRITTEN.

## 19. Performance and scalability analysis

NOT YET WRITTEN.

## 20. Cloud cost model

NOT YET WRITTEN.

## 21. Verification and test-quality assessment

NOT YET WRITTEN.

## 22. Build, dependency, release and supply-chain assessment

NOT YET WRITTEN.

## 23. Failure and chaos matrix

NOT YET WRITTEN.

## 24. Documentation and implementation drift

NOT YET WRITTEN.

## 25. Competitive architectural context

NOT YET WRITTEN.

## 26. Production adoption recommendation

NOT YET WRITTEN.

## 27. Production-readiness exit criteria

NOT YET WRITTEN.

## 28. Recommended next experiments

NOT YET WRITTEN.

## 29. Final risk register

NOT YET WRITTEN.

## 30. Evidence appendix

NOT YET WRITTEN.

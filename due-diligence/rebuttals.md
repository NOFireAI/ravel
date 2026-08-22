# Adversarial synthesis: falsification attempts on Critical and High findings

Method: for every P0/P1 finding a different reviewer (the panel chair with targeted code reading, or a dedicated skeptic agent) attempted to falsify it; the original evidence then had to survive the attempt. Each entry records the finding, the falsification attempt, the defense, and the resolution. Unresolved disagreement is preserved explicitly.

## R1. Memo B finding 1 (P1): orphan GC converts out-of-band commit-record loss into permanent deletion of acknowledged data

Falsification attempts:

1. "The scrub (ADR-0059) would detect the loss before the 25 h orphan gate fires." Checked: crates/ravel-maintain/src/scrub.rs walks objects identified by their CommitRecord (scrub.rs:10-13), so it verifies data-object integrity given a record; it cannot see a missing record at all. The scheduled scrubber (services/ravel-server/src/lib.rs:328-335, default period 7 days) does not close this. Attempt fails.
2. "`ravel-cli catalog verify` alarms on the state." Checked: verify (services/ravel-cli/src/catalog.rs:176-234) reports snapshot entries with no matching commit record but deliberately never fails on them, because retention legitimately produces that shape after folding. Detection exists only as non-failing report lines on an operator-run command. Attempt fails.
3. "The trigger is outside the store contract, so this is not a Ravel finding." Partially sustained: the trigger (lifecycle rule, prefix delete, persistent LIST omission) is external. But Ravel's sweeper is what converts recoverable metadata loss (data objects still present, reconstruction CLI exists) into unrecoverable data destruction, silently when below the breaker thresholds (50 count AND 10% ratio). The system's own ADR-0058 calls it the most dangerous flaw in the durability posture.

Resolution: P1 stands, scoped precisely: not a bug in the commit protocol, but a hazard amplifier in the deletion path with a detection gap below breaker thresholds. The window (grace + max_flush_lifetime, ~25 h) is the operator's entire reaction time, and nothing in-system alarms during it below the breaker. Recommended fix (report section 27): alarm on every orphan deletion batch above a small floor, and offer a quarantine-instead-of-delete mode (move to a `quarantine/` prefix for a second horizon) so the reaction window survives the sweep.

## R2. Memo C finding 1 (P1): compaction published more than 26 h behind the fold watermark poisons the folded snapshot; the superseded-input sweep then makes it a persistent 503

Falsification attempts:

1. "The re-resolve after SnapshotInvalidated falls back to listing and heals." Checked: the engine re-resolves exactly once (crates/ravel-query/src/engine.rs:1077-1142) and the re-resolve reads the same durable HEAD through resolve_snapshot_window's head cache (crates/ravel-catalog/src/snapshot_resolve.rs:359, 601-654); the fallback-to-listing path triggers on HEAD/part read failure, not on a resolved snapshot whose entries later 404. No healing path exists until a fold re-lists the hour, which the fixed 26 h window never does. Attempt fails.
2. "The maintain process forces a fold invalidation when it publishes a compaction record." Checked ADR-0065's invalidate hook: it drives maintain's own zone memoization, not the catalog fold's reconcile window. The fold's only triggers are the fixed window and the retention frontier band (fold.rs:949-1034). Attempt fails.
3. "catalog verify detects it." Same result as R1 attempt 2: entries with no commit record are reported but non-failing; worse, in this state data keys are reconstructed from snapshot entries without commit-record GETs, so the first user-visible signal is 503s. Attempt fails.
4. "The pinning test proves the project intends this, so it is a documented tradeoff, not a bug." Rejected: the test (fold.rs:3507-3557) pins the reconcile-window behavior, and the config comment (config.rs:76-85) claims safety under an assumption (record target hour within 26 h of watermark at publish) that nothing enforces; the sweep half of the interaction has no test at all. A documented tradeoff whose stated safety argument does not hold under ordinary operations (first maintain enablement on existing history, a one-day maintain outage) is a defect.

Resolution: P1 stands. Availability/visibility, not data loss (L1 parts conserve every sample; token reads unaffected). Blast radius: every non-token query touching the affected hours, persisting for weeks (retention-window tenants) or indefinitely (no-retention tenants). The asymmetry against retention's HEAD gate (retention.rs:563-577 vs sweep.rs:479-501) shows the fix shape already exists in-tree.

## R3. Memo C finding 2 (P1): erasure `.done`/`.dreq` lifecycle can re-serve erased-subject records from a stale folded snapshot

Falsification attempts:

1. "ADR-0064's completion gate resolves through the same resolver a query uses, so it cannot under-assert." Checked: the gate (erasure_rewrite.rs:1990-2115) resolves the live listing view; a query resolves the folded snapshot when the hour is at or below the watermark. Outside the reconcile window these two views diverge, which is exactly the case ADR-0064's own correction (0064:340-354) records as open. The gate satisfies only the weaker branch of the ADR's stated requirement. Attempt fails.
2. "The `.dreq` sweep waits for the protection horizon, which covers the stale window." Checked: the horizon anchors on `.done` created time plus protection_horizon (sweep.rs:1340-1390); the stale folded snapshot's lifetime is unbounded (until a fold re-lists the hour), so no horizon arithmetic covers it. Attempt fails.
3. "This requires erasure of data older than 26 h behind the watermark, which is rare." Rejected as mitigation: DSAR requests are mostly about historical data; the out-of-window case is the common case for erasure, not the corner.

Resolution: P1 stands for deployments using selective erasure for regulatory obligations; inapplicable otherwise. The compliance guarantee ("no query whose snapshot resolves after the request ack returns matching records") holds while the `.dreq` lives; the defect is that `.done` and the `.dreq` sweep can retire the filter while a stale snapshot still resolves pre-rewrite inputs. Same root cause as R2 (fold reconcile window); one fix addresses both.


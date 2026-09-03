# Resharding model traceability

Each row ties a TLA+ action or safety property in `OnlineResharding.tla` to
the Rust symbol that implements the same rule, the existing test that pins
that rule, and any test still worth adding. The middle column is the required
one and names a real symbol in the cited file; the harness
(`scripts/check-tla.sh traceability -a resharding`) resolves every
`crates/...` and `services/...` reference and every `::`-separated symbol in
it, so a rename on either side breaks this table.

The model is an abstraction: it counts shards and hours, not bytes, and it
mirrors the pure routing and scan-set arithmetic rather than the object-store
plumbing around it. Rows therefore map to the deterministic functions that
arithmetic lives in, not to the I/O wrappers that call them.

| TLA+ action or property | Meaning | Rust path and symbol | Existing test | New test worth adding |
|---|---|---|---|---|
| `ReqAppendOk`, `CasAppendNeverDiscards` | A generation is appended only by CAS on the current history; a lost race never drops a committed generation | `crates/ravel-catalog/src/provisioning.rs::append_generation` | `crates/ravel-catalog/src/provisioning.rs::append_generation_cas_conflict_on_concurrent_append` | none |
| `ReqAppendConflict`, `OneCasWinner` | Two concurrent reshards on the same base version: exactly one wins, the loser revalidates | `crates/ravel-catalog/src/provisioning.rs::append_generation` | `crates/ravel-catalog/src/provisioning.rs::create_if_absent_race_loser_revalidates_against_winner` | none |
| `ReqReject` past-activation guard | An activation hour not strictly in the future is rejected | `crates/ravel-catalog/src/provisioning.rs::append_generation` | `crates/ravel-catalog/src/provisioning.rs::append_generation_rejects_past_activation` | none |
| `ActiveCount` | Wall-clock active shard count is the newest generation whose activation hour has passed | `crates/ravel-catalog/src/provisioning.rs::active_shard_count` | `crates/ravel-catalog/src/provisioning.rs::active_shard_count_multiple_generations_at_boundaries` | none |
| `ScanCount`, `EveryAdmittedWriteInScanSet` | The read set unions every generation live within slack of the query hour, so an admitted straggler stays listed | `crates/ravel-catalog/src/provisioning.rs::scan_count` | `crates/ravel-catalog/src/provisioning.rs::scan_count_decrease_slack_window` | none |
| `RangeScanCount` | The scan count over an ingest-hour range takes the per-hour maximum, covering a straggler at any hour in the range | `crates/ravel-catalog/src/provisioning.rs::max_scan_count_over_range` | `crates/ravel-catalog/src/provisioning.rs::max_scan_range_decrease_inside_vs_outside_slack` | none |
| Slack constant `S` and its skew term | `DEFAULT_SCAN_SLACK_HOURS` folds in `TOLERATED_CLOCK_SKEW_HOURS`, the model constant `S` | `crates/ravel-catalog/src/provisioning.rs::DEFAULT_SCAN_SLACK_HOURS` | `crates/ravel-catalog/src/provisioning.rs::nf3_default_scan_slack_hours_includes_clock_skew_term` | none |
| `ShardCeiling`, `SafelyOldHeadRule` | A folded HEAD stamps a shard ceiling the reader may trust when its own view is safely old | `crates/ravel-catalog/src/provisioning.rs::shard_ceiling` | `crates/ravel-catalog/src/snapshot_resolve.rs::decrease_past_slack_head_validates_against_reader_ceiling` | none |
| `Acceptable`, `StaleReaderFailsClosed` | A reader HEAD is accepted only when it covers every generation active in its window; otherwise the read fails closed | `crates/ravel-catalog/src/snapshot_resolve.rs::head_generations_acceptable` | `crates/ravel-catalog/src/snapshot_resolve.rs::head_generations_predicate_covers_all_arms` | none |
| `ReaderQuery` HEAD fence | The reader validates its HEAD against the generation history before scanning | `crates/ravel-catalog/src/snapshot_resolve.rs::validate_head_against_generations` | `crates/ravel-catalog/src/snapshot_resolve.rs::older_head_reaching_unknown_generation_active_hours_is_rejected` | none |
| `AdmitAfterRefresh`, `TickActor` refresh | A writer routes on its cached generation view and swaps to the new count on refresh while the old set survives | `crates/ravel-ingest/src/generation.rs::route_cached` | `crates/ravel-ingest/src/generation.rs::refresh_switches_routing_and_old_set_survives` | none |
| `AdmitGrace`, `StaleWriterFailsClosed` | Grace routing continues only within the lead horizon of the last refresh; past it the writer stops | `crates/ravel-ingest/src/generation.rs::try_grace_extend` | `crates/ravel-ingest/src/generation.rs::try_grace_extend_refuses_once_horizon_crossed` | none |
| `AdmitFailClosed` fence | A refresh that fails past grace surfaces as a stale-view error rather than a stale route | `crates/ravel-ingest/src/error.rs::StaleProvisioningView` | `crates/ravel-ingest/src/generation.rs::cached_view_goes_stale_past_c` | a fence-off route today has no unit test; the model's no-writer-fence control stands in for it |
| `LeadCoversRefreshHorizon`, `MinLeadHours` | The activation lead is `ceil(C) + 1` so an activation always clears the refresh horizon | `crates/ravel-ingest/src/generation.rs::min_lead_hours` | `crates/ravel-ingest/src/generation.rs::min_lead_hours_matches_ceil_c_plus_one` | none |
| Reshard lead enforcement (CLI) | The reshard command refuses an activation inside `MIN_LEAD_HOURS` | `services/ravel-cli/src/provision.rs::reshard::MIN_LEAD_HOURS` | none in-file; exercised through the provision command tests | a direct unit test on the lead check would pin the boundary |
| `FlushOpen` ingest-hour pin, `FlushBound` | A flush pins its ingest hour at open and admission trails by the flush bound | `crates/ravel-ingest/src/config.rs::checked_ingest_hour_bucket` | `crates/ravel-ingest/src/config.rs::checked_ingest_hour_bucket_accepts_a_normal_reading` | none |
| Trailing-admission slack constant | `FLUSH_BOUND_SLACK_HOURS` is the model constant `FlushBound` | `crates/ravel-catalog/src/provisioning.rs::FLUSH_BOUND_SLACK_HOURS` | `crates/ravel-catalog/src/provisioning.rs::scan_count_decrease_slack_window` | none |
| `ResolveToken`, `TokenResolvesAcrossReshards` | A commit token resolves by exact key, so a straggler token still names its data after a decrease | `crates/ravel-commit/src/keys.rs::commit_key_for_token` | `crates/ravel-commit/src/keys.rs::commit_key_for_token_matches_commit_key` | none |
| Token shard identity | The commit token carries the shard index the record landed on | `crates/ravel-types/src/lib.rs::CommitToken` | `crates/ravel-commit/src/keys.rs::shard_out_of_range_is_rejected` | none |
| `DoAdmit` write path | Admission routes a point through the active generation set and writes it | `crates/ravel-ingest/src/router.rs::write_points` | covered by the ravel-ingest router integration tests | a focused test asserting the admitted shard equals the routed count would pin `EveryAdmittedWriteInScanSet` at the code level |

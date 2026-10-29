# Ravel Agents: storage protocol

Ravel Agents is a durable agent runtime that keeps all of its state in
object storage, under the tenant's prefix, using the same conditional-write
primitives as the telemetry catalog. This page is the normative description
of what is stored, how a command is published, and what a caller may rely
on. [ADR-3331](adrs/3331-ravel-agents-durable-agent-runtime.md) is the
decision record; the [Ravel Agents guide](guides/agents.md) is the
user-facing overview. Ravel Agents is not the MCP server: that read surface
is described in [the MCP guide](guides/mcp-server.md).

Status: the contracts (`ravel-agents-core`), the publication layer
(`ravel-agents-store`) and the coordinator (`ravel-agents-runtime`) are on
`main`, and `ravel-server --agents` serves them over HTTP: `create`, `send`,
`call`, `operation`, `result`, `resolve`, `cancel`, `state`, `events`,
`timers`, `timers/cancel`, `GET timers`, `effects`, `effects/{effect}` and
the `inspector` page for clients (`agents`); `worker/register`,
`worker/poll`, `worker/heartbeat`, `worker/commit`, `worker/step`,
`worker/effects/complete` and `worker/effects/fail` for workers
(`agents_worker`); and `reconcile` and `sweep` for operators (`agents_admin`), all under
`/api/v1/agents/` (the
[HTTP API reference](reference/http-api.md#agents-routes) lists them).
`services/ravel-server/tests/agents_e2e.rs` and
`services/ravel-server/tests/agents_effects_e2e.rs` drive them through a
real server, and `services/ravel-server/tests/agents_timers_e2e.rs` drives
the timer routes, `sweep` (`agents_admin`) and the inspector page.
Every claim below says which test pins it or which milestone delivers it;
the [guarantee table](#guarantees-and-limitations) collects them.

## Model

An agent is named by a kind and a key, each any non-empty UTF-8 string of at
most 256 bytes (`MAX_NAME_BYTES` in `crates/ravel-agents-core/src/keys.rs`;
test `keys::tests::names_are_non_empty_and_capped`). Its identity is

```
agent_id = hex(blake3("ravel-agent-v1" || len(kind) || kind || len(key) || key)[..16])
```

(ADR-3331 decision 2; tests `identity::tests::agent_id_is_the_documented_byte_stream`
and `identity::tests::length_prefixes_separate_component_boundaries`). No
caller text reaches an object key: agents are named by `agent_id` and
objects by the blake3-256 of their bytes
(`identity::tests::object_hash_is_plain_blake3_of_the_bytes`).

Each agent has exactly one mutable record, its HEAD. Everything else it
owns is an immutable, content-addressed object the HEAD references. A
message sent to an agent is accepted into its inbox with an `accept_seq`
and a receipt; turns run one at a time per agent in `accept_seq` order; a
turn consumes one input and publishes the new state, the input's result,
buffered events, timer changes and effect intents in one HEAD write
(ADR-3331 decisions 5 and 6; kernel test
`turn_commit_publishes_state_input_result_events_and_intents_together`).
There is no cross-agent ordering and no cross-agent transaction (ADR-3331
decision 5).

## Key layout

All keys sit under `t/<tenant_hash>/ag/` (ADR-3331 decision 2), so the
tenant's KMS routing and IAM scope apply to them. The builders in
`crates/ravel-agents-core/src/keys.rs` check every key with
`is_addressable_key` before any store call and return a typed `KeyError`
otherwise (test `keys::tests::every_built_key_is_addressable`; layout
pinned by `keys::tests::layout_matches_the_adr_table`).

| Key | Builder | Mutability | Write mode |
|---|---|---|---|
| `t/<tenant_hash>/ag/<agent_id>/head` | `head_key` | the one mutable record per agent | `CreateIfAbsent` on create, then `CasVersion` |
| `t/<tenant_hash>/ag/<agent_id>/o/<object_hash>` | `object_key` | immutable: payloads, state snapshots, outputs, index pages | `CreateIfAbsent`; on `AlreadyExists` the writer reads back and compares the content hash |
| `t/<tenant_hash>/ag/_wake/<due_bucket>/<agent_id>` | `wake_key` | advisory wake hint; may be absent, duplicated, stale or early | `Overwrite` |
| `t/<tenant_hash>/ag/_dir/<agent_id>` | `dir_key` | advisory listing entry | `CreateIfAbsent`, empty body |

`<due_bucket>` is `floor(due_unix_ns / 60_000_000_000)` written as 20
decimal digits (`WAKE_BUCKET_WIDTH_NS`), so LIST order is due order (test
`keys::tests::wake_buckets_list_in_due_order`); a negative due time is a
typed error (`keys::tests::negative_due_is_typed`). The listing prefixes are
`tenant_prefix`, `agent_prefix`, `wake_prefix` and `dir_prefix`.

ADR-3331 decision 2 says a wake hint is deleted once HEAD no longer needs
it. The gateway and query IAM templates grant `s3:DeleteObject` on
`t/*/ag/_wake/*` for the sweep's hint delete, and no role may delete any
other key under `ag/` (test
`agents_namespace_is_granted_to_gateway_and_query_roles_and_read_only_to_maintain`
in `crates/ravel-commit/tests/iam_templates.rs`). Live garbage collection of agent objects is out of
scope for this release (ADR-3331 decision 2).

Because IAM's `*` matches `/`, the template grant `t/*/ag/*` also reaches
the Parquet manifests of a table named `ag`. `ag` is therefore in
`IAM_GRANT_SEGMENTS` (`crates/ravel-pqtable/src/names.rs`) and is a reserved
table name; an existing table of that name stops resolving and
`ravel-cli parquet repair --stray` classifies its manifests as
`ReservedName` (ADR-3331 decision 2; tests
`names::tests::every_segment_a_template_grants_after_a_wildcard_is_reserved`
and `every_reserved_table_name_has_a_manifest_witness_a_template_grant_reaches`).
The per-role grants are in [deploy/iam/README.md](../deploy/iam/README.md#ravel-agents-tag).

## The HEAD record

HEAD is the `AgentHead` message in `proto/ravel/agents.proto`, modelled by
`Head` in `crates/ravel-agents-core/src/head.rs`. It is a CAS-mutable record
in ADR-0066's sense (amendment R1), so every additive field change bumps
`format_version` (ADR-3331 decision 3).

Encoding rules, each pinned by a `head::tests` test:

- `format_version` is field 1 and is gated before any other field is
  trusted. Version 1 (every list `INLINE`) and version 2 (every list
  `PAGED`) decode; a newer version is `UnsupportedFormat`, and a
  representation that does not match its version is refused
  (`a_newer_format_version_is_refused_before_the_rest_is_read`,
  `a_representation_that_does_not_match_the_version_is_refused`,
  `a_repeated_format_version_field_is_gated_on_its_decoded_value`). The
  encoder writes version 2 only; a version 1 binary refuses it as
  `UnsupportedFormat` (`a_version_one_reader_refuses_a_version_two_head`).
- The record ends with a `seal` field, `0x52414731` ("RAG1"); a missing or
  wrong seal is a typed error (`the_seal_is_the_last_six_bytes`,
  `missing_or_wrong_seal_is_typed`, `every_strict_prefix_is_a_typed_error`).
- The encoder builds a fresh message from `Head`, so a field this binary
  does not know is dropped on the next write
  (`an_unknown_field_within_version_2_is_dropped_on_re_encode`). This is
  why a newer version must be refused rather than read.
- Encoded HEAD is capped at 256 KiB (`MAX_HEAD_BYTES`); a transition past
  the cap fails `BudgetExceeded` (kernel test
  `logical_caps_and_the_byte_cap_are_typed`).
- `root_revision` increases on every published write, so two roots never
  encode to the same bytes and an ETag cannot alias two states
  (`two_roots_differing_only_in_root_revision_encode_differently`).
- The store uploads HEAD with a CRC32C checksum (`checksum` in
  `crates/ravel-agents-store/src/publish.rs`; ADR-3331 decision 3). No test
  asserts the checksum yet.

Fields of `Head` (format version 2):

| Field | Type | Meaning |
|---|---|---|
| `tenant` | `TenantHash` | the owning tenant |
| `kind` | `AgentKind` | the agent kind, stored raw |
| `key` | `AgentKey` | the agent key, stored raw |
| `incarnation` | `u64` | increases each time a tombstoned agent is created again; every handle carries it |
| `root_revision` | `u64` | increases by one on every published HEAD write |
| `owner_id` | `String` | the worker owning the agent in this epoch; empty when none has activated it |
| `epoch` | `u64` | ownership epoch; a takeover increments it and revokes the previous owner |
| `owner_lease_hint_unix_ns` | `i64` | advisory lease hint; never used for safety |
| `state_version` | `u64` | version of the agent state, advanced by each committed turn |
| `state` | `Option<ObjectRef>` | the state snapshot object (hash and size) |
| `next_accept_seq` | `u64` | the `accept_seq` the next accepted input gets |
| `active_turn` | `Option<ActiveTurn>` | the running turn: input `accept_seq`, turn id, base state version, handler name, version and digest, cancellation and block state, start time |
| `timer_generation` | `u64` | per-agent counter that numbers timer generations |
| `next_event_seq` | `u64` | sequence of the next committed event |
| `lifecycle` | `Lifecycle` | `Active` or `Tombstoned` |
| `quota` | `Quota` | logical counters: referenced bytes, mailbox depth, receipt count, timer count |
| `inbox` | `Vec<InboxEntry>` | accepted inputs not yet consumed: `accept_seq`, handler, source (message with request id and hash, or timer with id and generation), payload, accept time, failed attempts |
| `receipts` | `Vec<Receipt>` | request id, handler, request hash and `accept_seq` of every accepted message |
| `results` | `Vec<ResultEntry>` | per consumed input: status (`Completed`, `Failed`, `Cancelled`), output, error, turn id, dead-letter flag, finish time |
| `events` | `Vec<EventEntry>` | committed events: event seq, `accept_seq`, turn id, type, payload |
| `timers` | `Vec<TimerEntry>` | timer id, key, generation, due time, handler, payload, status (`Scheduled`, `Delivered`, `Cancelled`) |
| `steps` | `Vec<StepRecord>` | the durable step transcript: step id, key, tool name and version, input hash, status, output |
| `effects` | `Vec<EffectRecord>` | the effect journal: effect id, step key, tool identity, input hash, recovery policy, status (`Intent`, `Authorized`, `InDoubt`, `Completed`), attempt, result |
| `outbox` | `Vec<OutboxIntent>` | effect intents committed by a turn and not yet dispatched |
| `index` | `Index` | one `Directory` per paged list (`inbox`, `receipts`, `results`, `events`, `timers`; `AgentHead` fields 35 to 39): the `PageRef`s of its sealed pages in key order and `tail_from`, the first key the inline `Vec` above holds |

### Index pages

Format version 2 (ADR-3331 decision 3 and its index-page amendment) keeps
only the open tail of the inbox, receipts, results, events and timers in
HEAD. The rest sits in sealed index pages, `o/<object_hash>` objects that
hold the existing `InboxPage`, `ReceiptPage`, `ResultPage`, `EventPage` or
`TimerTable` message. The code is `crates/ravel-agents-core/src/pages.rs`.

- A page holds at most `Limits::max_page_entries` entries (default 64) and a
  list at most `Limits::max_index_pages` pages (default 512). Past either
  bound the command fails `BudgetExceeded` (`pages::tests::a_full_page_refuses_the_next_append_typed`,
  `pages::tests::a_full_directory_refuses_the_next_page_typed`).
- Nothing prunes the events, results or timers lists, so each stops at
  `max_index_pages * max_page_entries` entries (32,768 at the defaults):
  events, one or more per committed turn; results, one per settled input,
  so after that many lifetime inputs; timers, one entry per distinct timer
  key, which stays after the timer is delivered or cancelled (a reschedule
  replaces its key's entry; `max_timers` counts only scheduled timers). The
  publish that would seal a page past that bound is refused
  `BudgetExceeded`. The agent stays consistent but refuses new work for
  good: this release has no way to trim any of the three lists.
- Each `PageRef` records the page's first key, object, entry count, marked
  count and referenced bytes; a receipt page also carries the sorted 4-byte
  blake3 fingerprints of its request ids (a result page its turn ids', a
  timer page its timer ids'), and an events page the lowest and highest
  `accept_seq` of its events. The range is not a first value: a
  reconciliation's audit event carries its effect's input, which can be
  older than the events before it. The decoder refuses a range with its
  minimum above its maximum, or one on any other list
  (`pages::tests::an_inverted_or_misplaced_accept_range_is_refused`). A
  `PageRef` records no due time, since a sealed timer page never holds a
  scheduled timer; its old field number, 5, is reserved.
- A full page of the tail seals once its entries cannot change: inbox,
  receipts and events always; results below the oldest pending input;
  timers below the lowest generation still scheduled
  (`pages::tests::timer_pages_never_hold_a_scheduled_timer`,
  `pages::tests::results_seal_below_the_lowest_pending_input`).
- The store writes every sealed page with `CreateIfAbsent` before the CAS
  that first references it (store test
  `tests::pages::pages_are_written_before_the_cas_that_references_them`);
  a retry after a lost CAS writes byte-identical pages under the same hashes
  (`tests::pages::a_lost_cas_reuses_the_page_hash_on_retry`).
- A command GETs only the pages `Kernel::pages_needed` names. A new send
  reads no page, a repeat send or a receipt lookup at most one, a wake no
  timer page, and `read_events` reads by `event_seq` range
  (`tests::pages::send_receipt_lookup_and_wake_read_at_most_one_index_page_each`,
  `tests::pages::events_are_read_by_seq_range`). Receipts that share a
  fingerprint are each found by their own request id
  (`tests::pages::colliding_receipt_fingerprints_resolve_to_their_own_receipts`);
  a page the planner failed to name makes the command load every page and
  counts a `planner_misses`
  (`tests::pages::a_planner_miss_loads_every_page_and_is_counted`).
- No server or runtime path reads every page of every list. The
  runtime's reload and the sweep's per-agent visit read HEAD alone
  (`read_head_shallow`); a sweep visit to an agent with sealed pages is
  one GET (runtime test
  `sweep::tests::a_visit_reads_only_the_head_of_an_agent_with_sealed_pages`).
  A dispatch scan then attaches the inbox pages holding the first and the active
  input. Server test `agents_e2e::hot_routes_read_one_page_of_a_deep_history`
  (770 turns, 12 sealed pages per list) pins each route's GETs:
  - a result wait reads HEAD, one results page and the output;
  - a resolve over a sealed receipt reads HEAD, one receipts page and one
    results page;
  - an events page reads HEAD, the events pages from the one holding the
    cursor until it holds `limit` events (at most `ceil(limit / K) + 1`
    pages at `K` entries a page), and the payloads. A cursor into sealed
    history is served: the oldest retained seq is the first sealed page's
    first key;
  - an operation filter applies before the limit, and the read stops
    loading pages only once it holds `limit` matching events. It GETs
    only the events pages whose `accept_seq` range holds the operation,
    normally one, and skips the rest with no GET, the page that reports
    the operation terminal included. A page whose range a reconciliation
    audit event widened over the operation is read too, so the count is
    more than one only there, and at most `max_index_pages` (store tests
    `tests::pages::an_operation_read_skips_pages_outside_its_accept_range`,
    `tests::pages::an_audit_event_widens_its_page_range_and_is_read_for_its_operation`);
  - a poll reads HEAD twice plus the state and payload objects and no
    sealed page when the runtime holds the agent's root and the input is
    on the open inbox page; a cold root adds the scan's inbox pages and an
    input on a sealed page adds its page;
  - a state read attaches every inbox page, so its mailbox lists every
    queued input;
  - a timers read reads HEAD and attaches every timer page, so it lists
    the delivered and cancelled generations sealed there (server test
    `agents_timers_e2e::timers_route_lists_delivered_timers_on_sealed_pages`).

  `read_head_full`, the inspection read of the simulator, the tests and
  the CLI's `inspect`, `effects` and `operation` commands, GETs every
  page. The CLI's `list` reads HEAD alone and its `events` reads as the
  route does (`agents_commands::events_and_list_read_only_the_pages_they_print`).
- A version 1 HEAD migrates on the next command's own CAS; the HEAD it
  writes is version 2 and under the byte cap (kernel test
  `one_cas_migrates_a_maximal_inline_head_under_the_byte_cap`, store test
  `tests::pages::a_version_one_head_migrates_on_the_next_cas`). Reader and
  writer ship in one release, a pre-1.0 departure from ADR-0066's
  readers-first rule that rests on no released build carrying the agents
  crates: a version 1 binary refuses the version 2 HEAD as
  `UnsupportedFormat` with `RetryHint::Never` and never re-encodes it, and
  a writer rollback is impossible once a version 2 HEAD exists.

A HEAD is not deleted. `destroy` sets `Tombstoned`; `create` on a tombstoned
agent bumps `incarnation` in the same HEAD, and a handle from an earlier
incarnation is refused with `AgentGone` (kernel test
`a_tombstoned_agent_is_gone_until_recreated_under_a_new_incarnation`, store
test `create_on_a_tombstoned_agent_bumps_the_incarnation`).

## The publication rule

ADR-3331 decision 4, quoted:

> Every durable command runs the same skeleton in `ravel-agents-store`:
>
> 1. GET HEAD; keep `GetOutcome.version` as the CAS token.
> 2. Resolve the command's operation id against the receipt index. Same id
>    and same request hash: return the recorded handle or result. Same id,
>    different hash: `IdempotencyConflict`.
> 3. Validate preconditions against the root (incarnation, epoch, active
>    turn, base state version, cancellation state, quotas).
> 4. Build the immutable dependencies inside the memory budget.
> 5. PUT each dependency with `CreateIfAbsent`; on `AlreadyExists` read back
>    and compare the content hash.
> 6. CAS HEAD from the observed version to the new root with
>    `root_revision + 1`.
> 7. Return success only when the CAS returned `Ok`.
>
> `PreconditionFailed` means re-read and revalidate from step 1, up to a
> bounded attempt count; the command is never replayed against a root whose
> epoch, active turn or base state version differs from what it validated.
> A `Transient`, `Timeout` or `Throttled` error after the CAS is an unknown
> outcome: the caller re-reads HEAD and resolves its own operation id
> through the receipt index, which survives later root revisions. While the
> store stays unavailable the API returns `OutcomeUnknown` with the operation
> handle, never a manufactured success and never a fresh id.

`AgentStore` in `crates/ravel-agents-store` implements it in one private
skeleton (`publish.rs`). Steps 2 and 3 are the pure kernel,
`Kernel::apply(head, command, now_unix_ns, ids)`, which does no I/O. The
bounds:

- At most `MAX_CAS_ATTEMPTS` = 8 conditional writes in total for one
  command, each refusal re-running from step 1, then `StoreUnavailable`
  (`a_contended_head_gives_up_after_the_cas_bound` pins eight CAS calls).
- At most `MAX_READ_ATTEMPTS` = 3 attempts at one read or one dependency
  write that failed retryably. The test
  `a_head_not_found_blip_is_retried_as_a_transient_read` pins the retry; no
  test asserts the bound of 3.
- Every retry waits through an injected backoff that honours a
  `Throttled` answer's `retry_after_ms` as its floor
  (`a_retry_waits_through_the_backoff_seam`,
  `the_jittered_backoff_grows_to_its_cap_and_honours_retry_after`).
- An object larger than 4 MiB (`DEFAULT_MAX_OBJECT_BYTES`) is refused
  before any store call (`an_oversized_payload_is_refused_before_any_store_call`,
  `an_oversized_reference_is_refused_before_any_dependency_write`).

What the store tests pin about the rule:

- A duplicate send with the same request id and hash writes nothing and
  returns the same handle
  (`enqueue_with_the_same_request_id_and_hash_returns_the_same_handle`).
- A refused CAS re-reads and the command is revalidated against the new
  root (`a_failed_conditional_write_re_reads_and_succeeds_on_retry`,
  `a_contended_cas_uses_the_version_of_its_own_re_read`).
- An ambiguous CAS answer is not retried: the command returns
  `OutcomeUnknown` with its operation handle and issues no second CAS
  (`an_unknown_cas_outcome_is_never_retried`,
  `an_unknown_outcome_on_any_operation_command_carries_its_handle`).
- A CAS that applied but lost its response resolves to the recorded result
  through the receipt index, even after later revisions
  (`cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision`,
  `a_lost_write_is_not_mistaken_for_a_later_writer_with_the_same_id`,
  `resolve_keeps_the_handle_when_head_cannot_be_read`).
- A HEAD of a newer format is reported `UnsupportedFormat` and the store
  issues no PUT or CAS against it
  (`a_newer_head_format_is_unsupported_and_never_written`).
- A dependency whose stored bytes do not match their hash is a typed error
  (`a_corrupt_dependency_write_is_a_typed_error`,
  `a_corrupt_dependency_read_is_a_typed_error`).
- A create writes the `_dir` entry with `CreateIfAbsent` before the HEAD
  write, and a create whose entry cannot be written fails with
  `StoreUnavailable` and writes no HEAD
  (`a_create_whose_dir_entry_fails_writes_no_head`), so every agent with a
  HEAD has an entry (ADR-3331, the directory amendment to decision 8). A
  create that finds the agent already active re-creates a missing `_dir`
  entry with `CreateIfAbsent`
  (`a_create_that_finds_the_root_repairs_the_dir_entry`).
- The `_wake` hint is written after the HEAD write; a failure to write it
  does not fail the command, which still reports what its CAS published
  (`a_failed_wake_hint_does_not_fail_the_command`); the failure is logged,
  which no test asserts.

The store counts GET, PUT, CAS, LIST and conflicts per phase (resolve,
build, store, publish) in its own counters (`counters.rs`); the store tests
that name a count (the CAS, conflict and read counts above) assert it
exactly.

## Operation states

An accepted input is an operation. `operation_state` in
`crates/ravel-agents-core/src/state.rs` derives its state from HEAD
(ADR-3331 decision 5). `AgentStore::resolve` reports it; the store tests
assert the `Accepted` and `Cancelled` rows
(`cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision`,
`an_unknown_outcome_on_any_operation_command_carries_its_handle`), and
no test asserts `Running` or `Blocked` yet, so those two rows describe the
code, not a pinned behaviour:

| State | Code | When | Terminal |
|---|---|---|---|
| Accepted | `accepted` | the input is in the inbox and no turn is running it | no |
| Running | `running` | the active turn runs this input | no |
| Blocked | `blocked` | the active turn runs this input and is blocked (a non-deterministic replay) or one of its effects is in doubt | no |
| Completed | `completed` | a result entry with status `Completed` exists | yes |
| Failed | `failed` | a result entry with status `Failed` exists | yes |
| Cancelled | `cancelled` | a result entry with status `Cancelled` exists | yes |

Receipts and results are kept for the agent's lifetime in this release:
nothing in the kernel removes them, and results seal into index pages up to
the caps of the lifetime-limit row under Guarantees and limitations. An
agent at its mailbox cap refuses new sends rather than dropping anything
(ADR-3331 decision 5; kernel test `logical_caps_and_the_byte_cap_are_typed`).
The receipt cap refuses the same way in `Kernel::enqueue` (`BudgetExceeded`),
pinned end to end by `a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`.

A client holds an operation by an opaque handle: base64url without padding
over `v1:<kind>:<agent_id>:<incarnation>:<item>`, where the kind is `op`,
`step`, `effect` or `timer`. A token of another version decodes to a typed
unsupported-version error, not to a malformed one, and each handle has
exactly one token (`state::tests::unknown_version_is_typed_not_malformed`,
`a_decoded_token_is_the_canonical_one`, `handles_round_trip`).

## Typed errors

Every surface reports one of eleven kinds (ADR-3331 decision 11), each with a
fixed retry hint (`ErrorKind::retry` in `crates/ravel-agents-core/src/error.rs`;
tests `error::tests::codes_are_distinct_snake_case_and_round_trip` and
`every_kind_has_the_retry_hint_its_meaning_needs`). An error carries only
safe identifiers: hex hashes and handle tokens, never a raw key, payload or
tool input (`SafeIds`).

| Kind | Code | Meaning | Retry hint |
|---|---|---|---|
| `AgentGone` | `agent_gone` | the agent does not exist, is tombstoned, or the handle names an earlier incarnation | `never` |
| `IdempotencyConflict` | `idempotency_conflict` | a request id reused with a different request hash, or two different results for one attempt | `never` |
| `OutcomeUnknown` | `outcome_unknown` | the command may or may not have been published | `resolve_operation`: resolve the handle, do not resubmit under a new id |
| `OwnershipLost` | `ownership_lost` | the command's epoch, owner or active turn no longer matches HEAD, or the effect state does not admit the command (a reconcile of an effect that is not in doubt: runtime `reconcile_is_idempotent_and_audited_once`, e2e `a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`; a new attempt for an input being cancelled: runtime `a_new_attempt_for_a_cancelled_input_is_refused_and_the_completion_settles_cancelled`) | `never` |
| `IncompatibleHandler` | `incompatible_handler` | the handler version or digest differs from the turn's pin, or the worker has no handler for the input | `never` |
| `NonDeterministicReplay` | `non_deterministic_replay` | a step key reused with a different tool identity or input hash | `never` |
| `EffectInDoubt` | `effect_in_doubt` | a `manual` effect had an ambiguous attempt | `reconcile`: an operator must reconcile first |
| `MailboxFull` | `mailbox_full` | the mailbox is at its depth cap | `backoff`: retry the same request id later |
| `BudgetExceeded` | `budget_exceeded` | a logical quota or the HEAD byte cap would be exceeded | `never` |
| `StoreUnavailable` | `store_unavailable` | the store did not answer, or a stored record failed to decode | `backoff` |
| `UnsupportedFormat` | `unsupported_format` | a record carries a format version or representation this binary does not read | `never` |

The wire form carries the code and the retry hint
(`wire::fixtures::error_body_carries_the_code_and_retry_hint`). A
refusal of a lifetime cap also names the cap in `cap`, from `BudgetCap` in
the same file: `receipts` (`Limits::max_receipts`), `index_pages`
(`max_index_pages`, or one page's entries), `head_bytes` (`max_head_bytes`,
or the reserve the growth guard keeps under it), `mailbox`
(`max_mailbox_depth`, on `mailbox_full`) or `timers` (`max_timers`)
(`wire::fixtures::error_body_names_the_cap_a_budget_refusal_hit`; runtime
`each_cap_refusal_moves_its_counter_by_exactly_one` reaches all five). A
`budget_exceeded` for a per-command bound (a caller string, one turn's
events or intents, an exhausted counter) names none
(`a_per_command_refusal_is_not_counted_as_a_cap`).

## Wire

The request and response bodies are the `wire` types in
`crates/ravel-agents-core/src/wire.rs`, served under `/api/v1/agents` and
listed route by route in
[the HTTP API reference](reference/http-api.md#agents-routes). Their JSON
forms are pinned by the files in `sdk/fixtures/`, which
`wire::fixtures::fixtures_match_the_wire_types` regenerates and checks.

Fill. `GET /api/v1/agents/state` answers `usage`, the counts behind each
lifetime cap read from the agent's HEAD as stored: `receipts` against
`max_receipts`, the sealed pages of the results, events and timers lists
against `max_index_pages`, and the encoded HEAD size against
`max_head_bytes` (`a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`).
A list's inline entries are not counted, since only sealed pages approach
the page cap. `ravel-cli agents inspect` prints the same counts against the
default caps: it reads the bucket directly and cannot see a server's
`--agents-max-*` flags (`events_and_list_read_only_the_pages_they_print`). The sweep measures the
same counts for every agent from the HEAD each directory cycle already
reads, with no further request, and publishes the highest fill per list and
the agents at or above 75% of a list's cap once the cycle completes
(runtime `a_directory_cycle_publishes_the_highest_fill_and_the_near_cap_counts`,
`the_fill_is_published_only_when_a_directory_cycle_completes`;
`docs/guides/observability.md`, "Ravel Agents capacity").

Canonical step-input encoding. A step's `input_hash` is
`identity::step_input_hash(tool_name, tool_version, input)` over the bytes of
the input's canonical JSON: the SDK takes the input's JSON form (what
`JSON.stringify` would encode: `toJSON` applied, `undefined` members dropped,
`undefined` array items written as `null`), then writes it with object keys
sorted at every depth and no whitespace, and hashes those bytes. Two equal
inputs hash equal whatever their key order, and a value with no lossless JSON
form (a `Date` before `toJSON`, a `Map`, a `bigint`, a non-finite number) is
refused with `InvalidStepInput` rather than hashed (SDK `identity.test.ts`:
"canonical JSON sorts keys at every depth", "step id and step input hash
match the worker_poll_response fixture").

Steps. `worker/step` is the route that records a step: it journals the
step under the polled lease with its `input_hash` and recovery policy,
authorizes an attempt, and answers `StepRecordResponse` with the step, the
effect handle and the attempt to run. The worker reports that attempt's
result with `worker/effects/complete` or a tool failure with
`worker/effects/fail`. A failure is a typed result object, never read from
an output's shape: it leaves the step `started`, a replayed step answers it
as `failure` with no `output`, and `worker/poll` hands the step out as
`started` with no output, so a retried turn replays the failure
(`a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail`); an
output of exactly `{"error": "x"}` replays as that output
(`an_output_shaped_like_a_failure_replays_as_the_output`). A failure's
result object is a NUL byte, the ASCII text `ravel-agents-tool-failure-v1`,
a NUL byte, then the error as UTF-8 text (`FAILURE_RESULT_PREFIX` in
`crates/ravel-agents-runtime/src/effects.rs`); any other result object is
the output's JSON text, which never contains a NUL byte. ADR-3331 decision
7 does not define a result object's bytes, so this document does. Nothing
outside `ravel-agents-runtime` wrote result objects before failures were
stored this way, so no older object is read as a failure. A result for an
attempt another attempt's result already settled answers `late: true` and
is kept only as evidence. `reconcile` (`agents_admin`) settles a `manual`
effect in doubt. Its `retry` dispatches nothing: the next `worker/step`
for the step is answered the authorized attempt to run, once, whether it
comes from the owner the turn had at the reconcile or from a worker that
took the turn over after that owner's lease expired
(`a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`,
`a_reconcile_retry_is_handed_to_the_owner_after_a_takeover`). That
hand-over is kept only in the memory of the server process that answered
the reconcile. If that process restarts before the step, or the step is
served by another server process, the attempt reads as lost and a `manual`
effect is back in doubt: reconcile it again. A repeated `retry` is
answered as applied only while that process still holds its hand-over
unclaimed; a `retry` naming the attempt before one a worker's own step
authorized, such as an `at_least_once` attempt raised after a takeover, is
409 `ownership_lost` with no audit record and no hand-over
(`a_reconcile_retry_of_an_attempt_a_takeover_raised_is_ownership_lost`).
`effects/{effect}` reads each attempt, the
result and the evidence (all in `agents_effects_e2e`, one test per policy
and refusal), and lists at most the latest 64 attempts
(`agents_timers_e2e::effect_inspect_lists_at_most_the_latest_64_attempts`).
The serving process forgets an effect's evidence once a reconcile settles
it out of doubt (`runtime::tests::forgetting_an_effect_frees_its_slot_in_a_full_index`).
It names the evidence of at most 4096 effects per tenant. An effect that a
reported result, an operation status read or an inspection showed settled
(its result recorded, or its input finished or cancelled) stays listed
until the index is full, then gives its slot to a new effect; an effect
still in doubt keeps its slot
(`runtime::tests::a_full_index_gives_only_a_settled_effects_slot_away`,
`settled_effects_give_their_evidence_slot_to_an_effect_in_doubt`). A HEAD
read whose inbox has sealed pages it did not load shows no uncompleted
effect settled, since an input waiting to retry may sit on one of them
(`runtime::tests::an_effect_of_a_retrying_input_on_a_sealed_page_keeps_its_slot`).
A late result the index cannot list, because every slot holds an effect
still in doubt or its effect already lists 16 late results, is still
stored but not listed. Each one counts on
`ravel_agents_evidence_dropped_total`, and the process logs a warning
naming the tenant, the effect and the cap at most once a minute per tenant
(`a_late_result_the_full_evidence_index_drops_is_counted`,
`runtime::tests::every_late_result_the_index_cannot_list_is_counted`).

Timers. `timers` (`TimerRequest`) arms the agent's timer `timer_key` at
`due_unix_ns` and answers its id and generation (`TimerResponse`);
scheduling a key again bumps the generation and supersedes the earlier
one, so only the new generation fires, once
(`a_reschedule_supersedes_the_earlier_generation`). `timers/cancel`
(`CancelTimerRequest`) answers `CancelTimerResponse` with `outcome`
`cancelled` when it lands before the firing, `already_fired` when the
firing committed first and its input is already in the mailbox, and
`unknown` for a key never scheduled
(`cancel_after_fire_is_already_fired_and_before_fire_is_cancelled`).
`GET timers` (`TimersResponse`) lists the latest generation of each key as
`scheduled`, `delivered` or `cancelled`, sealed timer pages included
(`timers_route_lists_delivered_timers_on_sealed_pages`). A fired timer is an input like a
message: `GET state` lists it in `mailbox` with `source` `{"kind": "timer",
"timer", "generation"}`. Timer commands reach HEAD only through the
coordinator, and `FireTimer` only from its sweep. The sweep is the
authority: a `_wake` hint whose PUT failed costs nothing but latency, since
the directory half of a pass finds the agent and fires the timer once,
also in a process started after the one that scheduled it
(`due_timer_is_delivered_once_across_a_coordinator_restart_with_no_hint`).

Sweep. Each served tenant's coordinator runs a pass every
`agents.sweep_interval_ms` (0 runs none; worker polls and `sweep` still
do), and `sweep` (`agents_admin`) runs one pass over the caller's tenant
on demand and answers `SweepResponse`: the `_wake/` and `_dir/` LIST pages,
HEAD reads, hints read and deleted, timers fired, agents loaded and `_dir`
entries repaired. Those counts are the requests the store served for the
pass and the deltas of the `ravel_agents_sweep_*` families on `/metrics`
(`sweep_route_counts_match_the_store_and_needs_agents_admin`). The
interval task covers the tenants this process has served since it
started; a tenant no route has touched is swept by no one. It runs a pass
each interval with no poll and no `sweep` call
(`the_interval_sweep_runs_a_pass_per_interval_with_no_poll`). An idle
worker poll also runs a pass, at most once per `sweep_interval_ms`, and at
most once per 5000 ms of the agents clock when that is 0, so idle polls
never sweep on every wake
(`interval_zero_runs_no_periodic_pass_and_polls_sweep_at_the_floor`).

Events cursor. `events` (`EventsRequest`) pages committed events strictly
after `cursor`, the decimal committed event sequence, and `next_cursor`
resumes without a repeat or a gap. With `operation` set the page holds
only that operation's events, and the page that drains them once the
operation is finished carries `terminal` with its final status. A cursor
below the retained window is 410 with `CursorExpiredBody`: `code`
`cursor_expired`, `oldest_cursor` and `restart`
`restart_from_oldest_cursor`. A `limit` of 0 is 422
(`events_resume_from_a_cursor_to_terminal_and_expire_below_the_window`).
Nothing trims events from HEAD in this release, so the 410 is reachable
only once a later release retains a window.

Inspector. `GET inspector` (`agents`) serves one embedded HTML page with
no external asset, under a content security policy that allows no origin
but this one. The page asks for the caller's token and reads `state`,
`timers`, `effects`, `effects/{effect}` and `events` with it. It shows the
state version, the active turn, the mailbox, the timers, the effect-backed
steps, the effects with their status, and the events page by page, with
hashes and raw payloads folded under `<details>` and each payload preview
labelled "preview". A blocked agent shows its reason and the action that
unblocks it: `EffectInDoubt` (the `reconcile` bodies), a
`NonDeterministicReplay` or `IncompatibleHandler` turn (the handler version
and digest to deploy), or a cancel pending (wait for the worker). Its
marker ids and content type are pinned by
`inspector_is_served_to_agents_tokens_with_every_section` and
`agents::tests::inspector_serves_the_embedded_page_as_html`. Journaled
steps with no effect have no client route, so the page omits them. A
plain browser navigation sends no bearer token and is 401: open the page
from a client that sets `Authorization`.

Not found. An operation handle the agent never issued, one from an earlier
incarnation, or one for a tombstoned agent answers `AgentGone` (404) with
the handle itself in the safe ids, beside the agent id, so a caller can tell
which of its handles is gone without the server echoing any caller text
(`agents_e2e::unknown_operation_handle_is_agent_gone_with_the_handle`). A
token that does not decode as an operation handle is `AgentGone` with no
ids, since its text is not known to be safe to echo. A HEAD that is still
not found under a handle after the store's repeated reads answers
`OutcomeUnknown` (500) with the handle and `retry` `resolve_operation`, not
`AgentGone`: a HEAD is never deleted, so the server treats the miss as a
failed read, whether or not the agent was ever created
(`agents_e2e::a_not_found_blip_under_a_handle_is_outcome_unknown_not_agent_gone`).

## Clocks

Safety never depends on a clock: every exclusion is a CAS on HEAD, fenced by
incarnation, epoch, active turn and base state version (ADR-3331 decisions
6 and 8; kernel tests `every_fenced_command_refuses_a_stale_fence`,
`a_commit_after_takeover_is_ownership_lost`). Every time that enters a
decision comes through an injected `Clock` (`now_unix_ns`); the kernel
takes the time as a parameter. The one wall-clock read in the store seeds
the retry jitter (`backoff.rs`) and decides nothing.

Timer accuracy and takeover speed do depend on clocks. The assumption is
monotonic local clocks and bounded skew between coordinators (ADR-3331
decision 8). A wake hint is bucketed by minute and may be early or late; the
runtime's sweep makes a due timer fire without its hint, judged by one HEAD
read against the coordinator's injected clock, within the pass bound its
module documents (`a_schedule_whose_hint_was_never_written_still_fires_when_due`).
Work given to an agent through another server process reaches this
process's workers the same way: a poll issues no listing of its own
(`a_poll_issues_no_unbounded_list`), so the sweep's directory half finds
the agent within `ceil(agents / directory GETs per pass)` passes (at
least 32 directory GETs per pass by default, half of the pass's 64 HEAD
reads plus whatever the wake half left;
`a_bounded_pass_walks_the_directory_in_list_order_across_passes`), one pass
per sweep interval. One directory cycle reads every agent's HEAD once, so
this bound is observable on `/metrics`:
`ravel_agents_sweep_directory_cycles_total` counts the cycles the passes
completed, and `ravel_agents_sweep_last_directory_cycle_ms` is how long the
last full cycle took on the coordinator's clock, the largest over the
tenants this process sweeps
(`tests::a_directory_cycle_is_counted_and_timed_when_the_cursor_wraps`).
The owner lease hint in HEAD is advisory and revokes nothing; only an epoch
advance does (ADR-3331 decision 6).

## Projection

Committed history projects asynchronously into logs and traces (ADR-3331
decision 12). Each committed event becomes one log record and each committed
turn one span, under the `ravel-agents` service name. The record body reads
`agent event event_id=<id> agent_id=<agent> incarnation=<n> event_seq=<n>
accept_seq=<n> turn_id=<turn> type=<event type>` followed by the payload
part, and the same fields are attributes (`agent.event_id`, `agent.id`, and
so on). A committed event is queryable as a log line under its event id
through the server's own query path
(`committed_turn_is_queryable_as_a_log_with_its_stable_event_id_and_a_projection_outage_changes_no_state`
in `services/ravel-server/tests/agents_projection_e2e.rs`, which matches
`ravel_log_lines{__body__=~".*event_id=<id> .*"}` on `/api/v1/query`).

The event id is `blake3` over the tenant, the agent, its incarnation and the
event seq, so it is the same on every delivery attempt and in every process
(`ids_are_stable_and_separate_every_component`). Delivery duplicates, and
nothing rate-limits the duplicates. A projection cursor lives only in memory,
so every process that serves or sweeps a tenant projects each agent it
touches from that agent's first event, under the ids every earlier delivery
carried (`a_restart_repeats_history_under_the_same_ids`,
`a_restarted_projector_redelivers_history_under_the_same_ids`). A restart,
a second replica serving the same tenant, a sweep pass in another process
and a retried batch each repeat records. One agent holds at most 32,768
events (`max_index_pages` 512 times `max_page_entries` 64), so each process
can deliver up to 32,768 duplicate records per agent, once per process
start. Re-reading a full history costs about 520 GETs per agent: one
shallow HEAD per fill (eight fills at the default backlog of 4096) plus the
512 sealed event pages, and one more GET per event when the tenant's
payloads are carried. A fleet restart multiplies that by the agents the
restarted processes touch.

The event id is the deduplication key, carried as the `agent.event_id`
attribute. A query that must count each event once groups on it:

```sql
SELECT attrs['agent.event_id'] AS event_id,
       min(ts) AS ts,
       min(body) AS body
FROM logs
WHERE attrs['service.name'] = 'ravel-agents'
GROUP BY attrs['agent.event_id']
ORDER BY ts
```

The trace id is derived from the tenant, agent, incarnation and accept seq,
and the span id from the tenant, agent and turn id, so a turn's span and its
event records share a trace. A process emits one span per turn even when the
backlog bound splits the turn's events across two reads
(`a_turn_split_across_two_fills_is_one_span`).

Payloads are redacted by default: a record carries
`payload_hash=<blake3 hex> payload_size=<bytes> payload=redacted`, and the
payload text never leaves the agent store
(`a_redacted_record_carries_hash_and_size_and_never_the_payload`; the
acceptance test above finds no log line holding the payload text). A tenant
named by `--agents-projection-payload-tenant` (repeatable) gets
`payload=<text>` instead (`payload_text_is_projected_only_for_an_opted_in_tenant`).
The hash is a plain, unkeyed `blake3` of the payload bytes. It hides a
payload nobody can guess, not one drawn from a small set: anyone who can
query the tenant's logs can hash a candidate (`yes`, `no`, a short id) and
confirm it against `payload_hash`. Keep low-entropy secrets out of event
payloads, or treat the projected logs as readable by whoever can read them.

Where records go depends on the process. `--agents` runs only in `query`
and `all` modes (`agents_in_a_non_query_mode_is_refused`), and of those only
`all` holds an ingest buffer. So a `query`-mode process with `--agents` sets
`--agents-projection off`, points `--agents-projection-endpoint` at a
loopback gateway, or runs in `all` mode instead:

| Process | `--agents-projection` | `--agents-projection-endpoint` | Records go to | Health `mode` |
|---|---|---|---|---|
| holds an ingest buffer (`all`) | `auto` (default) | unset | this process's own log and span ingest routers | `in_process` |
| any | `auto` | plain `http://` on a loopback host | that endpoint over OTLP gRPC, tenant in the `x-ravel-tenant` header | `endpoint` |
| any | `auto` | another host, or any `https://` URL | refuses to start, naming the loopback rule | none |
| no ingest buffer (`query`) | `auto` | unset | refuses to start, naming both ways out | none |
| any | `off` | unset | nowhere | `off` |
| any | `off` | set | refuses to start: the two contradict | none |

So projection is never silently absent
(`a_process_that_cannot_project_refuses_to_start_and_off_is_reported`,
`agents_projection_target_resolves_or_refuses_naming_both_ways_out`). The
endpoint is loopback-only because the projector sends a tenant's history with
no credential, trusted on the tenant header alone; until the projector has a
credential model (ADR-3331, projection-as-built amendment), the receiving
gateway must resolve tenants from that header, which means running it with
`--dev-insecure-tenant-header`. Startup accepts that flag only when both
`--listen-http` and `--listen-grpc` bind a loopback address
(`dev_insecure_tenant_header_on_non_loopback_fails_validate`,
`dev_insecure_tenant_header_on_non_loopback_grpc_fails_validate`), so the
endpoint mode is a same-host development topology
(`agents_projection_endpoint_is_loopback_http_only`,
`a_query_mode_process_projects_to_a_loopback_gateway`). In production, run
the agents process in `--mode all`, or set `--agents-projection off`. The
in-process sink applies the tenant's IPCrypt rules like any other ingest, and
the default normalisation limits. A tenant whose rules cannot be resolved
fails the batch, which the projector retries.

"A projection failure never changes a committed transition" (ADR-3331
decision 12). The projector is a reader: its only store calls are HEAD,
event page and payload GETs, on a store instance of its own, so its requests
do not appear in the coordinator's `ravel_agents_store_*` counters. With the
sink failing across a turn, that turn's state, operation results,
coordinator writes and agent-key writes equal those of a run with no outage,
and once the sink recovers the missed events arrive once each under the ids
every failed attempt carried (the acceptance test above;
`a_failing_sink_changes_no_agent_state_and_retries_the_same_ids`). A failed
delivery is retried after one second, and the canonical history stays in the
agent store.

The ADR sets no bound on undelivered records; this implementation holds at
most `--agents-projection-backlog` records (default 4096) per tenant. At the
bound the projector stops reading and counts, the agents fall behind, and no
commit waits on it
(`a_full_backlog_counts_and_leaves_the_coordinator_untouched`,
`a_full_backlog_stops_reading_and_counts_and_never_stops_a_commit`).

Lag is reported on `GET /api/v1/agents/health` under `projection`:
`lag_max` is the largest committed-minus-projected event count over the
agents this process has seen, beside `backlog`, `backlog_bound`,
`events_projected`, `spans_projected`, `sink_failures`, `backlog_full`,
`read_failures` and `records_rejected`, and on `/metrics` as the
`ravel_agents_projection_*` family (`docs/guides/observability.md`;
`projection_metrics_scrape_matches_health_after_an_outage`).

A record's time comes from committed data where the projector can find it.
An event entry records no time of its own, so a record takes its turn's
`finished_unix_ns` from the turn's result, and a turn's span starts and ends
at that time. The projector reads HEAD shallow, without the sealed index
pages, so it finds the result only while it sits on the open results page;
for a turn whose result has been sealed away (an agent with more than
`max_page_entries` results committed since), the record takes the
projecting process's clock when it read the event. The `agent.time_source`
attribute says which: `turn_finished` or `read`. Nothing is restamped at
delivery, so a batch retried across an outage keeps the time it was read
with (`a_record_carries_its_turns_finish_time_not_the_delivery_time`). The
tradeoff: a re-projected old turn keeps its old time, so the log ingest
window can refuse it as too old. That refusal is permanent, counted in
`records_rejected` and reported in `last_rejection`, and the agent's later
events still project. Reading the sealed results pages instead would put
the right time on every record at up to 512 more GETs per full-history
re-read.

Limits of this version: a record carries the turn's accept seq as its
message id and the turn id, and no per-event state version or handler
version, since an event entry records neither; effects are not projected.
Per-event versions need their own ADR (ADR-3331, amendment "Projection as
built").

## Guarantees and limitations

One row per behaviour ADR-3331 promises, or a limitation of this release
against it. Status is `shipped` (on `main`, pinned by the tests in the row),
`blocked` (a known defect: a test pinning the promised behaviour exists and is
ignored until the fix lands) or `deferred` (not built in this release; the
row says what happens instead). The protocol or model column names the TLA+
row in `formal/tla/agents/traceability.md`, or the kernel test where the TLA+
model has no action for the behaviour. The fault schedule column names a
`ravel-agents-sim` schedule (`module::test` under
`crates/ravel-agents-sim/src/schedules/`), which drives the production store
and runtime. The backend contract column names the section of
`docs/object-store-contract.md` the behaviour rests on, and the qualification
probe where one exists. "none" in a column is a gap, with the reason given in
the cell. End-to-end tests are in `services/ravel-server/tests/agents_*.rs`.

| Behaviour | Status | Protocol or model row | Fault schedule | Backend contract | Evidence |
|---|---|---|---|---|---|
| All agent objects live under `t/<tenant>/ag/`; keys carry no caller text | shipped | `TenantIncarnationIsolation` | `errors::hostile_keys_are_agent_gone` | Addressable keys (`OperationsRefuseUnaddressableKeys`) | `keys::tests::layout_matches_the_adr_table`, `every_built_key_is_addressable` |
| `ag` is a reserved Parquet table name | shipped | none: a naming rule outside the protocol | none: no fault can change a name | none: no store call | `names::tests::every_segment_a_template_grants_after_a_wildcard_is_reserved` |
| gateway and query roles may list, read and write `t/*/ag/*` and delete `t/*/ag/_wake/*` only; maintain may list and read; no other role may delete | shipped | none: IAM templates are deployment, not protocol | none: the sim store has no roles | Mandatory capabilities (production) | `agents_namespace_is_granted_to_gateway_and_query_roles_and_read_only_to_maintain` |
| A newer HEAD format is refused before any field is read and is never written | shipped | `ReadHead` | `errors::unsupported_head_format_is_refused` | Rules for callers | `head::tests::a_newer_format_version_is_refused_before_the_rest_is_read`, store `a_newer_head_format_is_unsupported_and_never_written` |
| A command succeeds only when its HEAD CAS returned `Ok` | shipped | `CasHead`, `CasHeadLostResponse` | `senders::lost_cas_response_across_seeds` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | `an_unknown_cas_outcome_is_never_retried` (an ambiguous CAS is not reported as success), `a_failed_conditional_write_re_reads_and_succeeds_on_retry` (a refused CAS is not) |
| An ambiguous CAS returns `OutcomeUnknown` with a handle that resolves later | shipped | `Resolve`, `RequestIdentityStable` | `senders::lost_cas_response_followed_by_later_revisions_still_resolves_the_same_operation` | Runtime qualification (`ConsistentReadAfterWrite`) | `cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision`; e2e `resolve_after_a_lost_send_ack_returns_the_same_operation` |
| Duplicate send with the same id and hash returns the same handle; a different hash conflicts | shipped | `EnqueueCas`, `RequestIdentityStable` | `senders::same_request_twice_is_a_duplicate`, `senders::same_id_different_payload_conflicts`, `senders::duplicate_and_conflict_across_seeds` | Runtime qualification (`ConsistentReadAfterWrite`) | store `enqueue_with_the_same_request_id_and_hash_returns_the_same_handle`, kernel `retries_are_idempotent_and_conflicts_are_typed` |
| Concurrent senders each get a distinct accept seq and no send is lost | shipped | `NoLostConcurrentEnqueue`, `MonotonicIdentity` | `senders::competing_senders_take_distinct_accept_seqs`, `senders::competing_senders_across_seeds` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the schedules above against the production store |
| Mailbox, receipt, timer, index page and HEAD byte caps refuse with a typed error naming the cap, never drop | shipped | kernel `logical_caps_and_the_byte_cap_are_typed` | `errors::full_mailbox_is_refused`, `errors::oversized_payload_is_budget_exceeded` | none: the caps are checked on the decoded HEAD before any write | store `a_full_mailbox_is_refused_before_any_write`, runtime `each_cap_refusal_moves_its_counter_by_exactly_one`; e2e `a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter` (a send past `max_receipts` is 413 with `cap` `receipts`) |
| Agent lifetime limit: one incarnation accepts at most `--agents-max-receipts` sends (1,024 by default), and the results, events and timers lists stop at 512 pages of 64 entries each; nothing trims them, and no route destroys an agent in this release, so a capped agent stays capped | shipped (limitation) | kernel `logical_caps_and_the_byte_cap_are_typed`, `head_stays_under_the_cap_with_every_list_at_max_pages` | none: no schedule fills a list to its cap | none: a HEAD-content limit | e2e `a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter`; kernel `retries_are_idempotent_and_conflicts_are_typed` (a duplicate uses no receipt); `docs/guides/operations/agents.md`, "Long-lived agents" |
| Receipts and list entries are trimmed or expire, so an agent can live indefinitely | deferred | none: not built | none: not built | none: not built | the lifetime limit row above is what happens instead |
| Each agent's fill against its lifetime caps, and every cap refusal, is reported | shipped | none: reporting, not protocol | none: the sim asserts no metric | Runtime qualification (`CrossPageListing`): the fill is read on the directory cycle | e2e `a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter` (the four families on `/metrics` and the state route's `usage`); runtime `a_directory_cycle_publishes_the_highest_fill_and_the_near_cap_counts` (the cycle issues one HEAD GET an agent and no other), `the_fill_is_published_only_when_a_directory_cycle_completes` |
| Bounded paged indexes (format version 2): results, events and timers seal into immutable pages and HEAD stays under 256 KiB | shipped | kernel `head_stays_under_the_cap_with_every_list_at_max_pages` | `workers::one_worker_turn_issues_the_documented_requests` | Semantics adapters MUST honor (`ConditionalWriteCreateIfAbsent` for a page) | Index pages above; e2e `events_page_after_the_durable_cursor_without_duplicates` |
| A turn commit publishes state, result, events and intents in one write, and rebases over a concurrent send | shipped | `TurnCommitAtomic`, `CompleteTurn`, `CompleteTurnRebase`, `PublishedReferencesComplete` | `races::enqueue_racing_completion_rebases_and_keeps_the_input`, `races::enqueue_racing_completion_across_seeds` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `turn_commit_publishes_state_input_result_events_and_intents_together`, `a_commit_rebases_onto_a_root_that_moved_without_touching_the_turn` |
| A stale owner cannot commit or heartbeat after a takeover | shipped | `NoStaleOwnerCommit`, `Takeover` | `workers::stale_owner_commit_after_takeover_is_fenced`, `workers::suspended_worker_commit_is_refused_after_takeover`, `workers::worker_kills_across_seeds` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `a_commit_after_takeover_is_ownership_lost`, `every_fenced_command_refuses_a_stale_fence`; e2e `send_then_result_through_real_server_survives_worker_replacement` (the replaced worker's commit and heartbeat both answer 409 `ownership_lost`) |
| A takeover retried after its CAS lost a race does not fence a turn that began in between | blocked | `Takeover` | `effects::takeover_retried_on_fresh_head_fences_a_live_turn` (ignored: the retry re-applies the takeover on the fresh HEAD and fences the live turn) | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the ignored schedule is the test; `crates/ravel-agents-sim/README.md`, "Known gaps" |
| A poll whose `BeginTurn` CAS lost its answer twice settles the outcome instead of handing `OutcomeUnknown` to the worker | blocked | `BeginTurn`, `CasHeadLostResponse` | `workers::a_lost_begin_turn_answer_after_a_spent_retry_is_settled_by_the_poll` (ignored), `workers::a_lost_begin_turn_answer_after_a_spent_retry_escapes_the_poll_today` (pins today's behaviour) | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the two schedules; `crates/ravel-agents-sim/README.md`, "Known gaps" |
| Failed turns retry under a new turn id, then dead-letter | shipped | `FailTurn` | `effects::journaled_step_with_a_failed_attempt_replays_the_failure` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `failed_turns_retry_under_a_new_turn_id_then_dead_letter`; e2e `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` (a retryable failure is accepted and the turn runs again under a new turn id); no e2e test reaches the dead letter |
| A retry reuses the failed attempt's step and effect identities | shipped | `RecordStep`, `StepTranscriptCompatible` | `effects::journaled_step_with_a_failed_attempt_replays_the_failure` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `a_retry_reuses_the_effect_and_step_identities_of_the_failed_attempt` (ADR-3331 step identity amendment); e2e `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` |
| An effect is dispatched only after its authorization committed, and its policy decides redispatch | shipped | `DispatchRequiresCommittedAuthorization`, `PolicyControlsRedispatch`, `AuthorizeFirst`, `Reauthorize` | `effects::provider_idempotency_kills_across_seeds`, `effects::at_least_once_duplicate_execution_is_reachable_and_manual_policy_never_redispatches` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | e2e `provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts`, `completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn` |
| A `manual` effect in doubt blocks the turn until reconciled | shipped | `AdminAuthorize`, `AdminRecordResult`, `PolicyControlsRedispatch` | `effects::manual_killed_after_execution`, `effects::manual_suspended_before_result_publication` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `a_manual_effect_goes_in_doubt_and_blocks_the_turn_until_reconciled`; e2e `manual_blocks_until_reconciled_and_audits_the_decision_once` |
| A step replayed with a different input blocks the turn | shipped | `ReplayStepDrift` | `effects::handler_version_change_while_a_turn_is_suspended` | none: decided on the decoded HEAD | kernel `a_step_replayed_with_another_input_blocks_the_turn`; e2e `a_drifted_replay_is_non_deterministic_and_blocks_the_turn` |
| A result reported for an effect comes from its execution; a late or conflicting one is evidence or a conflict | shipped | `EffectResultFromExecution`, `RecordHeldResult` | `effects::late_result_after_cancel_is_evidence`, `effects::conflicting_results_for_one_attempt_are_idempotency_conflict`, `effects::at_least_once_late_result_after_takeover_is_evidence` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the schedules; e2e `provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts` |
| A reconcile `retry` is dispatched once by the next step for its key in the same server process, under the owner at the reconcile or one that took the turn over since; a restart of that process, or a step served by another one, loses it and a `manual` effect is in doubt again | shipped (limitation: the hand-over is held in memory) | `AdminAuthorize`, `Reauthorize` | `effects::manual_killed_after_execution` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | e2e `a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`, `a_reconcile_retry_is_handed_to_the_owner_after_a_takeover`; runtime `a_reconciled_attempt_survives_a_takeover_before_the_next_step`, `concurrent_steps_dispatch_a_reconciled_attempt_once`, `a_hand_over_refused_after_its_claim_is_released_and_never_dispatched_twice`. The loss on a restart is not tested: a restarted coordinator holds no record, and runtime `manual_latches_in_doubt_after_a_coordinator_restart_and_is_never_redispatched` shows what a `manual` attempt with no record does |
| Two admins reconciling one effect are told apart, so an unknown-outcome retry does not adopt the other admin's attempt | deferred (needs a caller identity in the HEAD format) | `AdminAuthorize` | none: the sim has one admin | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | runtime `a_reconcile_another_admin_landed_first_is_not_dispatched_again` (the owner still dispatches the attempt once); the audit record names the tenant and capabilities, not the person |
| The SDK pairs each step with its effect record on the server and drops its in-memory step journal | deferred | none: SDK, not protocol | none: the sim drives no SDK | none: no store call | `sdk/typescript/packages/agents/README.md`, `MemoryStepJournal` |
| A cancel of an input no turn has bound answers `requested: false` | blocked | `Cancel`, `CancellationRespected` | `races::cancel_of_an_accepted_input_reports_requested_false` (ignored: production answers `requested: true`) | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the ignored schedule; cancel of a running turn is shipped (`cancel_of_a_running_turn_settles_it_cancelled_at_commit`, `races::cancel_racing_completion_across_seeds`) |
| A timer fires once; a cancel after the firing reports `AlreadyFired` | shipped | `TimerGenerationDeliveredOnce`, `FireTimer`, `CancelTimer` | `timers::fire_cancel_race_across_seeds`, `timers::a_cancel_after_the_fire_is_already_fired`, `timers::two_sweepers_on_one_hint_fire_it_once` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | kernel `timers_fire_once_and_a_late_cancel_reports_already_fired`; e2e `cancel_after_fire_is_already_fired_and_before_fire_is_cancelled`, `a_reschedule_supersedes_the_earlier_generation` |
| A timer fires without its wake hint | shipped | `TimerDue`, `WriteHint` (store half), `FireTimer` (sweep half) | `timers::a_crash_between_the_root_write_and_the_hint_write_still_fires`, `timers::lost_hint_puts_across_seeds` | Runtime qualification (`ConsistentListAfterWrite`, `LexicographicListingOrder`, `CrossPageListing`); `list_after` in Semantics adapters MUST honor | e2e `due_timer_is_delivered_once_across_a_coordinator_restart_with_no_hint` (every hint PUT refused, the scheduling process shut down, a fresh process over the same store fires it once); runtime `a_lost_hint_is_found_by_the_directory_half`, `a_schedule_whose_hint_was_never_written_still_fires_when_due`; the directory half reaches the agent within `ceil(agents / directory GETs per pass)` passes (`a_bounded_pass_walks_the_directory_in_list_order_across_passes`); every agent with a HEAD has a `_dir` entry because a create writes it first (store `a_create_whose_dir_entry_fails_writes_no_head`), a hint-reached agent whose entry went missing gets it back (`a_hint_reached_agent_gets_its_missing_dir_entry_back`), and a listing page of unaddressable keys still moves the cursor (`a_wake_page_of_unaddressable_keys_moves_the_cursor_past_it`, `a_dir_page_of_unaddressable_keys_moves_the_cursor_past_it`) |
| Wake hints are deleted when no longer needed | shipped | `HintObservedEarly` | `timers::a_premature_hint_fires_nothing_and_is_kept`, `timers::duplicate_hint_delivery_across_seeds` | Runtime qualification (`DeleteVisibility`) | runtime `fire_and_cancel_race_resolves_at_head_and_a_stale_hint_never_fires_a_cancelled_generation`, `duplicate_hints_for_one_generation_fire_it_once`; a hint is deleted only after a HEAD read in the same pass shows no timer armed in its bucket, and a premature one is kept (`a_premature_hint_fires_nothing_and_is_kept`) |
| A send accepted by one process for an agent another process could serve is found within ceil(N/32) sweep passes at the 5 s interval | shipped (limitation: discovery delay) | `SweepRead`/`SweepFire` (design row, no Rust row) | none: the sim runs one coordinator per schedule except `workers::cold_restart_with_a_second_coordinator` | Runtime qualification (`CrossPageListing`) | `a_bounded_pass_walks_the_directory_in_list_order_across_passes`; `benchmarks/agents/REPORT.md`, "Discovery" (100 agents 11.5 s, 1,000 agents 2.5 min, 10,000 agents 26.0 min); a single process wakes at once (`a_waiting_poll_wakes_on_send_and_the_wait_is_capped`) |
| A send writes a wake hint so another process finds the agent on its next pass | deferred | `WriteHint` covers timers only | none: not built | none: not built | the discovery row above is what happens instead |
| A sweep pass is bounded: at most 8 LIST pages and 64 HEAD reads, whatever the number of agents | shipped | `TimerDue` | `timers::a_lost_hint_put_is_delivered_by_the_sweep_within_its_interval` | Runtime qualification (`CrossPageListing`) | `a_bounded_pass_walks_the_directory_in_list_order_across_passes`, `a_poll_issues_no_unbounded_list`, e2e `sweep_route_counts_match_the_store_and_needs_agents_admin`, `the_interval_sweep_runs_a_pass_per_interval_with_no_poll` |
| Destroy tombstones; recreate bumps the incarnation and refuses old handles | shipped (kernel and store only: no route, CLI command or SDK call destroys an agent in this release) | `Destroy`, `Recreate`, `TenantIncarnationIsolation`, `MonotonicIdentity` | none: no schedule destroys an agent; the reference machine checks it on its own (`recreate_after_destroy_refuses_the_old_handle`) | Semantics adapters MUST honor (`ConditionalWriteCreateIfAbsent`) | kernel `a_tombstoned_agent_is_gone_until_recreated_under_a_new_incarnation`, store `create_on_a_tombstoned_agent_bumps_the_incarnation` |
| Kernel sequences keep the HEAD invariants | shipped | kernel `kernel_sequences_keep_the_head_invariants` | every schedule checks the reference machine's invariants after each step | none: in-memory property test | kernel `kernel_sequences_keep_the_head_invariants`; sim `reference_machine_invariants_hold` |
| The production store and runtime are checked against the independent reference machine under faults | shipped | the reference machine in `ravel-agents-sim` | every schedule module: `effects`, `errors`, `races`, `senders`, `storm`, `timers`, `workers` | Runtime qualification (the sim store models the probed semantics) | `crates/ravel-agents-sim/src/schedules/mod.rs`; `workers::cold_restart_across_seeds`; the gaps the machine cannot express are listed in `crates/ravel-agents-sim/README.md`, "Known gaps" |
| An applied commit whose acknowledgement was lost, followed by a takeover, can answer `ownership_lost` | shipped (limitation: known false negative) | `CompleteTurnLostResponse` | `workers::lost_commit_ack_then_takeover_is_a_known_false_negative` | Semantics adapters MUST honor (`ConditionalWriteCasVersion`) | the schedule pins it |
| The HEAD publish's conditional writes match the in-memory oracle on a real S3 store | shipped | `CasHead` | none: runs against RustFS, not the sim | Runtime qualification (`ConditionalWriteCasVersion`, `ConcurrentCreateIfAbsentSingleWinner`) | `cas_version_on_absent_key_matches_memory_oracle_on_rustfs`, `create_if_absent_collision_matches_memory_oracle` (see Testing against a real store) |
| A recovery storm leaves telemetry ingest within its own budget | shipped | none: a resource bound, not protocol | `storm::recovery_storm_leaves_telemetry_ingest_within_its_own_budget`, `storm::recovery_storms_across_seeds` | none: the bound is on request counts | the schedules |
| HTTP routes for create, send, call, operation, result, resolve, cancel, state, events and the worker register, poll, heartbeat and commit | shipped | `Ack` | `workers::one_worker_turn_issues_the_documented_requests` | none: the routes add no store call of their own | e2e `send_then_result_through_real_server_survives_worker_replacement`, `call_heartbeat_and_cancel_through_real_server`, `cancel_of_a_running_turn_settles_it_cancelled_at_commit`, `events_page_after_the_durable_cursor_without_duplicates`, `resolve_after_a_lost_send_ack_returns_the_same_operation`; mounting `agents_router_mounts_on_both_listeners_only_with_feature_and_flag` |
| HTTP routes for steps, effect results, effect inspection and reconciliation (`worker/step`, `worker/effects/complete`, `worker/effects/fail`, `effects`, `effects/{effect}`, `reconcile`) | shipped | `RecordStep`, `AdminAuthorize`, `AdminRecordResult` | `effects::cancels_across_seeds` | none: the routes add no store call of their own | e2e `completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn`, `provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts`, `manual_blocks_until_reconciled_and_audits_the_decision_once`, `a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`, `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` |
| HTTP routes for timers and the sweep | shipped | `ScheduleTimer`, `CancelTimer` | `timers::fire_cancel_race_across_seeds` | none: the routes add no store call of their own | e2e `due_timer_is_delivered_once_across_a_coordinator_restart_with_no_hint`, `cancel_after_fire_is_already_fired_and_before_fire_is_cancelled`, `a_reschedule_supersedes_the_earlier_generation`, `sweep_route_counts_match_the_store_and_needs_agents_admin` |
| Every route refuses a token lacking its capability with 403, naming the capability | shipped | none: authorization sits in front of the protocol | none: the sim has no tokens | none: no store call | server `every_route_refuses_a_token_lacking_its_capability` sends every route in the `ROUTES` table, whose third column is the route's capability, a token holding every capability but that one and expects 403; e2e `capabilities_are_enforced_on_both_listeners` (`agents`, `agents_worker`), `manual_blocks_until_reconciled_and_audits_the_decision_once` (`agents_admin` on `reconcile`), `sweep_route_counts_match_the_store_and_needs_agents_admin` (`agents_admin` on `sweep`), `inspector_is_served_to_agents_tokens_with_every_section` (`agents` on `inspector`) |
| A HEAD that cannot be read under an operation handle answers `outcome_unknown`, not `agent_gone` | shipped | `ReadHead`, `Resolve` | `errors::missing_dependency_is_store_unavailable` | Runtime qualification (`ConsistentReadAfterWrite`) | e2e `a_not_found_blip_under_a_handle_is_outcome_unknown_not_agent_gone`; an unknown handle is `agent_gone` (`unknown_operation_handle_is_agent_gone_with_the_handle`) |
| `--agents-max-mailbox`, `--agents-max-receipts` and `--agents-max-timers` set the kernel caps | shipped | kernel `logical_caps_and_the_byte_cap_are_typed` | `errors::full_mailbox_is_refused` | none: HEAD-content limits | the server passes them as the kernel limits of every tenant's store, and the kernel refuses past a cap with a typed error; e2e `a_sweep_cycle_renders_the_fill_families_and_the_refusal_counter` sets `max_receipts` to 4 and is refused at the fifth send |
| `--agents-max-agents`, `--agents-max-state-bytes` and `--agents-memory-budget` are enforced | deferred | none: not built | none: not built | none: not built | the routes do not enforce these three flags: they are parsed and reported on `GET /api/v1/agents/health` and `/metrics` only (`agents_flags_parse_and_reach_query_budgets`, `health_reports_feature_mode_and_every_setting`) |
| Admission: activation and long-poll lanes refuse past their limit with 503 and take no query permit | shipped | none: admission sits in front of the protocol | none: the sim has no admission | none: no store call | e2e `admission_refusal_is_503_and_takes_no_query_permit` |
| Committed history projects into logs and traces under a stable event id, redacted by default, and a projection outage changes no agent state | shipped | none: projection reads committed history and is outside the protocol (ADR-3331 decision 12) | none: the sim does not project | Conditional reads (the projector's HEAD and page GETs) | e2e `committed_turn_is_queryable_as_a_log_with_its_stable_event_id_and_a_projection_outage_changes_no_state`, `payload_text_is_projected_only_for_an_opted_in_tenant`, `a_full_backlog_counts_and_leaves_the_coordinator_untouched`, `a_process_that_cannot_project_refuses_to_start_and_off_is_reported`, `a_restart_repeats_history_under_the_same_ids` in `agents_projection_e2e.rs`; see Projection above |
| Projection to a gateway on another host, with a credential | deferred | none: not built | none: not built | none: not built | the endpoint is loopback `http://` only (`agents_projection_endpoint_is_loopback_http_only`) and the receiving gateway must run `--dev-insecure-tenant-header`; see Projection above |
| MCP tools for agents | shipped | none: a client of the routes | none: the sim drives no MCP client | none: no store call of its own | `ravel_agent_send_with_an_idempotency_key_requires_the_agents_capability_and_returns_the_same_operation_twice`, `tools_list_with_agents_adds_six_tools_inside_the_budget` in `mcp_agents_e2e.rs`; in a build with the `mcp` feature, which the published image does not include |
| `ravel-cli agents` commands (dev, scaffold, list, inspect, events, reconcile) | shipped | none: a client of the store and routes | none: the sim drives no CLI | Runtime qualification (`CrossPageListing`, for `list`) | `services/ravel-cli/tests/agents_commands.rs`: `list_pages_are_bounded_and_resume_at_the_cursor`, `dev_command_refuses_the_memory_store_and_inspect_reads_head_from_the_bucket`, `scaffold_runs_the_sdk_generator_and_refuses_a_non_empty_dir_before_spawning`, `reconcile_posts_the_decision_with_the_admin_token` |
| The published `ravel-server` image is built with the `agents` feature | shipped | none: packaging | none: packaging | none: packaging | the root `Dockerfile` feature list, the README support matrix row and `docs/internal/development.md` name the same features |
| Live garbage collection of agent objects | deferred (out of scope for this release) | none: not built | none: not built | none: not built | ADR-3331 decision 2; destroy tombstones and leaves the objects |

## Testing against a real store

Two checks run the agents store on the compose RustFS
(`deploy/docker-compose/rustfs.yml`) instead of `MemoryStore`, and k8s-nightly's
`agents-chaos` job runs both every night (ADR-3331 decision 13). The chaos
script needs GNU date and bash 4.4 or later, so a local run is a Linux run.
To run them locally, start the store with
`docker compose -f deploy/docker-compose/rustfs.yml up -d rustfs` and create
the bucket with
`AWS_ACCESS_KEY_ID=ravel AWS_SECRET_ACCESS_KEY=ravel-dev-secret AWS_DEFAULT_REGION=us-east-1 scripts/ci-create-bucket.sh http://127.0.0.1:9000 ravel-chaos`,
which gives `ravel-chaos` the Object Lock, versioning and lifecycle settings
the nightly job's bucket carries.

`RAVEL_REAL_STORE_URL=http://127.0.0.1:9000 cargo test -p ravel-server --features agents --test agents_real_store -- --nocapture`
runs `services/ravel-server/tests/agents_real_store.rs`; without the
variable, each of its two tests prints a `skipping agents real-store` line and
passes. `cas_version_on_absent_key_matches_memory_oracle_on_rustfs` runs the
HEAD publish's five conditional writes (a `CasVersion` on an absent key, a
`CreateIfAbsent` on it, a matching and a stale `CasVersion`, and a
`CreateIfAbsent` on the present key) on RustFS and on `MemoryStore` and
asserts both give `[PreconditionFailed, Ok, Ok, PreconditionFailed,
AlreadyExists]`. `create_if_absent_collision_matches_memory_oracle` races two
`CreateIfAbsent` puts of different bytes on one key, at 64 bytes and at one
byte over the multipart threshold, and asserts on both stores that one writer
wins, the loser gets `AlreadyExists`, and the key holds the winner's bytes
unmixed.

`scripts/chaos/agents-replace-worker.sh` starts `ravel-server --agents`
(`CHAOS_AGENTS_SERVER_BIN`, built with `--features agents`) against the same
store and prints every figure it asserts as `KEY=VALUE`; with the store
already running, `rustfs_up` reuses it and empties the bucket. Its
replace-worker scenario SIGKILLs worker A inside an `at_least_once` step and
asserts that a resend under the same request id returns the same operation
with `OP1_RETRIED_DUPLICATE=true`, that worker B takes the same turn and
effect over with `B_STEP_ATTEMPT=2`, and that `RECOVERY_FROM_GRANT_MS`, from
A's grant to B's assignment, lies within [39000, 60000] (the 30 s lease plus
10 s grace, both counted from the grant, less 1 s for reading the grant time
after the server set it, plus one 20 s long poll). It also
asserts `GRANT_TO_KILL_MS`, from A's grant to its SIGKILL, at no more than
10000, so A dies while its lease is live. Its restart-server scenario SIGKILLs
and restarts the server with worker C's turn in flight and asserts
`RESTART_READY_MS`, from the SIGKILL to the restarted server's `/readyz`, at
no more than 30000 (C's lease), `OP2_STATUS_AFTER_RESTART=running`, that C
commits under its original lease
with `TAKEOVERS_AFTER_RESTART=0` and `TURNS_COMMITTED_AFTER_RESTART=1`, and
that the state version moves from `STATE_VERSION_BEFORE_RESTART=1` to
`STATE_VERSION_AFTER_RESTART=2`, so the result is written once.
`scripts/chaos/agents-replace-worker.test.sh` runs those assertions on canned
figures with no store, including a resend that mints a new operation and a
restart that duplicates the result, both of which must fail; ci.yml's
doc-scripts job runs it on every pull request.

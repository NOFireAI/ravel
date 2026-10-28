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
`effects` and `effects/{effect}` for clients (`agents`); `worker/register`,
`worker/poll`, `worker/heartbeat`, `worker/commit`, `worker/step`,
`worker/effects/complete` and `worker/effects/fail` for workers
(`agents_worker`); and `reconcile` for operators (`agents_admin`), all under
`/api/v1/agents/` (the
[HTTP API reference](reference/http-api.md#agents-routes) lists them).
`services/ravel-server/tests/agents_e2e.rs` and
`services/ravel-server/tests/agents_effects_e2e.rs` drive them through a
real server. Timers and the sweep have no route yet.
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
- Each `PageRef` records the page's first key, object, entry count and
  referenced bytes; a receipt page also carries the sorted 4-byte blake3
  fingerprints of its request ids, a timer page its earliest due time.
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
- No production path reads every page. The runtime's reload, the sweep's
  per-agent visit and the directory scan read HEAD alone
  (`read_head_shallow`); a scan then attaches the inbox pages holding the
  first and the active input. On the server a result wait reads HEAD, one
  results page and the output; an events page reads HEAD, the page holding
  the cursor and the payloads; a poll reads HEAD twice plus the state and
  payload objects and no sealed page (server test
  `agents_e2e::hot_routes_read_one_page_of_a_deep_history`, 769 turns, 12
  sealed pages per list). `read_head_full`, the inspection read of the
  simulator and the tests, GETs every page.
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

Receipts and results are kept for the agent's lifetime in this release
(nothing in the kernel removes them; paged indexes are milestone 6). An
agent at its mailbox cap refuses new sends rather than dropping anything
(ADR-3331 decision 5; kernel test `logical_caps_and_the_byte_cap_are_typed`).
The receipt cap refuses the same way in `Kernel::enqueue` (`BudgetExceeded`),
but no test asserts that refusal yet; it is test debt for the steps and
effects task (T14 of the epic).

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
(`wire::fixtures::tests::error_body_carries_the_code_and_retry_hint`).

## Wire

The request and response bodies are the `wire` types in
`crates/ravel-agents-core/src/wire.rs`, served under `/api/v1/agents` and
listed route by route in
[the HTTP API reference](reference/http-api.md#agents-routes). Their JSON
forms are pinned by the files in `sdk/fixtures/`, which
`wire::fixtures::tests` regenerates and checks.

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
effect is back in doubt: reconcile it again. `effects/{effect}` reads each attempt, the
result and the evidence (all in `agents_effects_e2e`, one test per policy
and refusal).

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
The owner lease hint in HEAD is advisory and revokes nothing; only an epoch
advance does (ADR-3331 decision 6).

## Guarantees and limitations

Milestones are those of ADR-3331 decision 15. "In the crates" means the
property holds in `ravel-agents-core`, `ravel-agents-store` or
`ravel-agents-runtime` on `main` and
is pinned by their own tests. "End to end" means a test in
`services/ravel-server/tests/agents_e2e.rs` or
`services/ravel-server/tests/agents_effects_e2e.rs` pins it through the HTTP
routes of a running server.

| Property | Status | Evidence |
|---|---|---|
| All agent objects live under `t/<tenant>/ag/`; keys carry no caller text | in the crates (M0) | `keys::tests::layout_matches_the_adr_table`, `every_built_key_is_addressable` |
| `ag` is a reserved Parquet table name | on main | `names::tests::every_segment_a_template_grants_after_a_wildcard_is_reserved` |
| gateway and query roles may list, read and write `t/*/ag/*` and delete `t/*/ag/_wake/*` only; maintain may list and read; no other role may delete | on main (templates) | `agents_namespace_is_granted_to_gateway_and_query_roles_and_read_only_to_maintain` |
| A newer HEAD format is refused before any field is read and is never written | in the crates (M0, M1) | `head::tests::a_newer_format_version_is_refused_before_the_rest_is_read`, store `a_newer_head_format_is_unsupported_and_never_written` |
| A command succeeds only when its HEAD CAS returned `Ok` | in the crates (M1) | `an_unknown_cas_outcome_is_never_retried` (an ambiguous CAS is not reported as success), `a_failed_conditional_write_re_reads_and_succeeds_on_retry` (a refused CAS is not) |
| An ambiguous CAS returns `OutcomeUnknown` with a handle that resolves later | in the crates (M1) | `cas_applied_with_lost_response_is_resolved_by_operation_id_after_a_later_revision` |
| Duplicate send with the same id and hash returns the same handle; a different hash conflicts | in the crates (M1) | store `enqueue_with_the_same_request_id_and_hash_returns_the_same_handle`, kernel `retries_are_idempotent_and_conflicts_are_typed` |
| Mailbox and HEAD byte caps refuse with a typed error, never drop; the receipt cap refuses the same way in code but has no test yet (test debt for T14) | in the crates (M1) | kernel `logical_caps_and_the_byte_cap_are_typed`, store `a_full_mailbox_is_refused_before_any_write` |
| A turn commit publishes state, result, events and intents in one write, and rebases over a concurrent send | in the crates (M1) | kernel `turn_commit_publishes_state_input_result_events_and_intents_together`, `a_commit_rebases_onto_a_root_that_moved_without_touching_the_turn` |
| A stale owner cannot commit or heartbeat after a takeover | in the crates (kernel); end to end | kernel `a_commit_after_takeover_is_ownership_lost`, `every_fenced_command_refuses_a_stale_fence`; e2e `send_then_result_through_real_server_survives_worker_replacement` (the replaced worker's commit and heartbeat both answer 409 `ownership_lost`) |
| Failed turns retry under a new turn id, then dead-letter | in the crates (kernel); the retry end to end | kernel `failed_turns_retry_under_a_new_turn_id_then_dead_letter`; e2e `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` (a retryable failure is accepted and the turn runs again under a new turn id); no e2e test reaches the dead letter |
| A retry reuses the failed attempt's step and effect identities | in the crates (kernel); end to end | kernel `a_retry_reuses_the_effect_and_step_identities_of_the_failed_attempt` (ADR-3331 step identity amendment); e2e `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` |
| A `manual` effect in doubt blocks the turn until reconciled | in the crates (kernel); end to end | kernel `a_manual_effect_goes_in_doubt_and_blocks_the_turn_until_reconciled`; e2e `manual_blocks_until_reconciled_and_audits_the_decision_once` |
| A step replayed with a different input blocks the turn | in the crates (kernel); end to end | kernel `a_step_replayed_with_another_input_blocks_the_turn`; e2e `a_drifted_replay_is_non_deterministic_and_blocks_the_turn` |
| A timer fires once; a cancel after the firing reports `AlreadyFired` | in the crates (kernel); end to end in M4 | kernel `timers_fire_once_and_a_late_cancel_reports_already_fired` |
| A timer fires without its wake hint | in the crates (runtime sweep); end to end in M4 | runtime `a_lost_hint_is_found_by_the_directory_half`, `a_schedule_whose_hint_was_never_written_still_fires_when_due`; the directory half reaches the agent within `ceil(agents / directory GETs per pass)` passes (`a_bounded_pass_walks_the_directory_in_list_order_across_passes`); every agent with a HEAD has a `_dir` entry because a create writes it first (store `a_create_whose_dir_entry_fails_writes_no_head`), a hint-reached agent whose entry went missing gets it back (`a_hint_reached_agent_gets_its_missing_dir_entry_back`), and a listing page of unaddressable keys still moves the cursor (`a_wake_page_of_unaddressable_keys_moves_the_cursor_past_it`, `a_dir_page_of_unaddressable_keys_moves_the_cursor_past_it`) |
| Wake hints are deleted when no longer needed | in the crates (runtime sweep); end to end in M4 | runtime `fire_and_cancel_race_resolves_at_head_and_a_stale_hint_never_fires_a_cancelled_generation`, `duplicate_hints_for_one_generation_fire_it_once`; a hint is deleted only after a HEAD read in the same pass shows no timer armed in its bucket, and a premature one is kept (`a_premature_hint_fires_nothing_and_is_kept`) |
| Destroy tombstones; recreate bumps the incarnation and refuses old handles | in the crates (M1) | kernel `a_tombstoned_agent_is_gone_until_recreated_under_a_new_incarnation`, store `create_on_a_tombstoned_agent_bumps_the_incarnation` |
| Kernel sequences keep the HEAD invariants | in the crates (M1) | kernel `kernel_sequences_keep_the_head_invariants` |
| The production store and runtime are checked against the independent reference machine | M1 (simulation drivers, T13) | the reference machine exists in `ravel-agents-sim` and is tested on its own (`reference_machine_invariants_hold`); nothing drives the production crates through it yet |
| HTTP routes for create, send, call, operation, result, resolve, cancel, state, events and the worker register, poll, heartbeat and commit | end to end | e2e `send_then_result_through_real_server_survives_worker_replacement`, `call_heartbeat_and_cancel_through_real_server`, `cancel_of_a_running_turn_settles_it_cancelled_at_commit`, `events_page_after_the_durable_cursor_without_duplicates`, `resolve_after_a_lost_send_ack_returns_the_same_operation`; mounting `agents_router_mounts_on_both_listeners_only_with_feature_and_flag` |
| HTTP routes for steps, effect results, effect inspection and reconciliation (`worker/step`, `worker/effects/complete`, `worker/effects/fail`, `effects`, `effects/{effect}`, `reconcile`) | end to end | e2e `completion_from_a_dead_worker_attempt_records_the_effect_and_the_new_owner_commits_the_turn`, `provider_idempotency_redispatches_the_same_attempt_and_the_late_result_counts`, `manual_blocks_until_reconciled_and_audits_the_decision_once`, `a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`, `a_failed_effect_replays_as_the_steps_failure_and_the_turn_may_fail` |
| A reconcile `retry` is dispatched once by the next step for its key in the same server process, under the owner at the reconcile or one that took the turn over since; a restart of that process, or a step served by another one, loses it and a `manual` effect is in doubt again | end to end (the hand-over, with and without a takeover); in the crates (the takeover, a race of two steps, a refused step handing it back) | e2e `a_reconcile_retry_hands_the_new_attempt_to_the_owners_next_step`, `a_reconcile_retry_is_handed_to_the_owner_after_a_takeover`; runtime `a_reconciled_attempt_survives_a_takeover_before_the_next_step` (the lease expires after the reconcile and the new owner's first step dispatches attempt 2 once, the effect reads authorized at attempt 2), `concurrent_steps_dispatch_a_reconciled_attempt_once`, `a_hand_over_refused_after_its_claim_is_released_and_never_dispatched_twice`. The loss on a restart is not tested: a restarted coordinator holds no record, and runtime `manual_latches_in_doubt_after_a_coordinator_restart_and_is_never_redispatched` shows what a `manual` attempt with no record does |
| `agents` on `send`, `agents_worker` on `worker/poll` and `agents_admin` on `reconcile` are checked end to end; every other handler passes its capability to the same admission prologue, pinned by reading, not by a test | end to end (the three routes named); in the crates (the prologue) | e2e `capabilities_are_enforced_on_both_listeners`; `agents_admin` on `reconcile` in `manual_blocks_until_reconciled_and_audits_the_decision_once`; the per-route table test is T19 |
| A HEAD that cannot be read under an operation handle answers `outcome_unknown`, not `agent_gone` | end to end | e2e `a_not_found_blip_under_a_handle_is_outcome_unknown_not_agent_gone`; an unknown handle is `agent_gone` (`unknown_operation_handle_is_agent_gone_with_the_handle`) |
| HTTP routes for timers and the sweep | M4 | ADR-3331 decision 9; no route yet (`a_path_outside_the_route_table_is_404`) |
| `--agents-max-mailbox`, `--agents-max-receipts` and `--agents-max-timers` set the kernel caps | on main | the server passes them as the kernel limits of every tenant's store, and the kernel refuses past a cap with a typed error (`logical_caps_and_the_byte_cap_are_typed`); no e2e test sets one below its default |
| `--agents-max-agents`, `--agents-max-state-bytes` and `--agents-memory-budget` are enforced | follow-up | The routes do not enforce these three flags: they are parsed and reported on `GET /api/v1/agents/health` and `/metrics` only (`agents_flags_parse_and_reach_query_budgets`, `health_reports_feature_mode_and_every_setting`). Enforcing them is left to a follow-up. |
| Projection of history into logs and traces | M5 | ADR-3331 decision 12 |
| Bounded paged indexes (format version 2) | M6 | ADR-3331 decision 3 |
| Live garbage collection of agent objects | out of scope for this release | ADR-3331 decision 2 |

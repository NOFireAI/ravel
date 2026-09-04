---------------------------- MODULE CommitProtocol ----------------------------
(***************************************************************************)
(* Commit publication, acknowledgement, retry and read-your-write.         *)
(*                                                                         *)
(* ABSTRACTION BOUNDARY                                                    *)
(*                                                                         *)
(* Modelled: the two-object commit as the ingest path performs it. A flush *)
(* pins its identity before any store call, PUTs its data object with      *)
(* CreateIfAbsent, PUTs its commit record with CreateIfAbsent, and only    *)
(* then acknowledges. Crashes, lost responses, retries of the same pinned  *)
(* flush, the flush-lifetime deadline, the acknowledgement timeout, the    *)
(* per-signal multi-shard outcome, the logs and spans idempotency marker,  *)
(* commit-token resolution with its four outcomes, and a read-path query   *)
(* that answers from the store rather than from what a writer believed.    *)
(*                                                                         *)
(* Assumed, not checked:                                                   *)
(*   - Data-object PUT idempotency. `put_data_object` returns success on   *)
(*     AlreadyExists with no read-back, so nothing can detect a key bound  *)
(*     to different bytes. Safety rests on the pinning invariant plus the  *)
(*     key layout (writer, epoch, seq, content hash), taken as given here. *)
(*     The commit record's idempotency IS checked: its AlreadyExists path  *)
(*     reads the winner back and compares content hashes.                  *)
(*   - The segment encoder. Bytes are an abstract content element and a    *)
(*     content hash is that element; no encoding is modelled.              *)
(*   - The object store honours its own contract. Every store operation    *)
(*     comes from RavelObjectStore, which encodes the contract document.   *)
(*                                                                         *)
(* Out of scope:                                                           *)
(*   - Commit-record reconstruction (ADR-0058), which derives a record's   *)
(*     created_unix_ns from the store's advisory last_modified. No         *)
(*     property here reads last_modified and that path is not modelled.    *)
(*   - Segment contents, series identity, query evaluation. A query asks   *)
(*     only whether a commit is visible.                                   *)
(*   - Cross-shard atomicity and ordering, which Ravel does not offer. The *)
(*     model must REACH a state with one shard durable and another not;    *)
(*     NoCrossShardAtomicity is stated as that reachability obligation.    *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Writers,        \* flush actors; one carries a (writer_id, epoch) pair
    Shards,         \* shard indices one request fans out over
    Contents,       \* abstract payloads; a payload is also its content hash
    MaxRetries,     \* per-flush store-retry budget (RetryPolicy::max_attempts)
    FlushLifetime,  \* max_flush_lifetime, in abstract ticks
    MaxTicks,       \* clock bound, to keep the state space finite
    Signal,         \* "metrics" | "logs" | "spans"
    HasIdemKey,     \* TRUE when the request carried an idempotency key
    \* Negative-control switches. Each defaults to FALSE in every correct
    \* configuration and is flipped by exactly one negative cfg.
    CommitBeforeData,       \* publish the record without the data object
    SkipHashCompare,        \* accept AlreadyExists without comparing hashes
    AckAtEnqueue,           \* acknowledge before anything is durable
    MarkerAfterFirstShard,  \* write the marker before every shard is durable
    RetryDedups,            \* a retry after a lost ack silently deduplicates
    QueryReadsDataDirectly, \* a query answers from the data object, skipping
                            \* the commit record
    CheckQuery              \* the read-path query is modelled at all. FALSE
                            \* in every cfg that does not list
                            \* NoUncommittedDataVisible among its INVARIANTs:
                            \* RunQuery's single firing can land at any of the
                            \* behaviour's reachable states, and that timing
                            \* choice alone enlarges the state space enough to
                            \* stop exhaustive.cfg and dedup-mutant.cfg from
                            \* converging. A cfg that never asks the question
                            \* does not need to pay for the answer.

ASSUME Writers # {}
ASSUME Shards # {}
ASSUME Contents # {}
ASSUME MaxRetries \in Nat
ASSUME FlushLifetime \in Nat /\ FlushLifetime > 0
ASSUME MaxTicks \in Nat
ASSUME Signal \in {"metrics", "logs", "spans"}
ASSUME HasIdemKey \in BOOLEAN

\* Only logs and spans carry idempotency markers; the metrics path has none.
MarkersApply == HasIdemKey /\ Signal \in {"logs", "spans"}

\* A pinned flush is one (writer, shard) pair. The writer carries writer_id and
\* epoch; seq is allocated at pin time and never reused, so within one model run
\* a pair names one flush.
FlushIds == Writers \X Shards

\* The client request a flush serves. A fresh pin serves its own request; a
\* retry after a lost acknowledgement serves the ORIGINAL one. The sentinel is
\* shaped like a flush id so TLC never compares a string with a tuple.
NoReq == <<"norequest", "norequest">>

NoC == "nocontent"
AllContent == Contents \cup {NoC}

\* Object keys: a data object and a commit record per flush, plus one marker.
\* Every key is a (flush, tag) pair so the key space is one uniform type.
ObjKeys == FlushIds \X {"d", "c", "m"}
DataKey(f)   == <<f, "d">>
CommitKey(f) == <<f, "c">>
MarkerKey    == <<CHOOSE f \in FlushIds : TRUE, "m">>

VARIABLES
    \* --- store state, shared with the RavelObjectStore instance -----------
    store, lastModified, versionCounter, uploads, listState,
    \* --- protocol state ---------------------------------------------------
    phase,      \* [FlushIds -> Phases]
    pinned,     \* [FlushIds -> AllContent]  the content pinned at flush open
    openedAt,   \* [FlushIds -> Nat]         flush-open tick
    retries,    \* [FlushIds -> Nat]         store retries spent
    clock,      \* Nat
    shardDead,  \* [Shards -> BOOLEAN]  set by split brain, cleared by nothing
    ackKind,    \* [FlushIds -> {"none","strict","buffered","timeout","error"}]
    marker,     \* "absent" | "written"
    \* --- witnesses: what a store operation actually returned --------------
    lastPut,     \* the commit PUT's own outcome, read off the store
    publishedAt, \* [FlushIds -> 0..MaxTicks+1] tick the record write landed
    tombstoned,  \* SUBSET FlushIds: buckets retention has tombstoned
    superseded,  \* SUBSET FlushIds: records a compaction or rewrite replaced
    retryOf,     \* [FlushIds -> FlushIds \cup {NoReq}] the earlier attempt a
                 \* flush is a client retry of. Only ClientRetry sets it, so
                 \* it is what makes a duplicate a duplicate rather than two
                 \* unrelated writes that happened to carry equal content
    tokenResult, \* [FlushIds -> the answer a commit-token query gave, WITH the
                 \* store state it saw: an answer is judged against the store
                 \* as it was when the query ran, because a token that is not
                 \* yet visible is a retryable failure, not a stale result
    queried,     \* BOOLEAN: whether the read-path query has run this behaviour
    queryAnswer  \* SUBSET FlushIds: the flushes the read-path query returned

Store == INSTANCE RavelObjectStore
            WITH Keys <- ObjKeys, Content <- AllContent,
                 NoContent <- NoC, Clients <- Writers

Phases == {"idle", "pinned", "data", "committed", "acked",
           "abandoned", "stopped", "retired"}
AckKinds == {"none", "strict", "buffered", "timeout", "error"}

\* The store's own variables, named locally: an INSTANCE's tuple cannot be
\* primed through the instance prefix.
sVars == <<store, lastModified, versionCounter, uploads, listState>>

protoVars == <<phase, pinned, openedAt, retries, clock, shardDead,
               ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          phase, pinned, openedAt, retries, clock, shardDead,
          ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* The commit PUT witness: what the caller asked for, what the store already
\* held, and what the operator returned. Invariants read THIS and the store,
\* never a field an action asserts about itself.
NoPut == [kind |-> "none", flush |-> CHOOSE f \in FlushIds : TRUE,
          mine |-> NoC, stored |-> NoC, outcome |-> "none"]

TypeOK ==
    /\ Store!StoreTypeOK
    /\ phase \in [FlushIds -> Phases]
    /\ pinned \in [FlushIds -> AllContent]
    /\ openedAt \in [FlushIds -> 0..MaxTicks]
    /\ retries \in [FlushIds -> 0..MaxRetries]
    /\ clock \in 0..MaxTicks
    /\ shardDead \in [Shards -> BOOLEAN]
    /\ ackKind \in [FlushIds -> AckKinds]
    /\ marker \in {"absent", "written"}
    /\ lastPut \in [kind: {"none", "commit"}, flush: FlushIds,
                    mine: AllContent, stored: AllContent,
                    outcome: {"none", "Ok", "AlreadyExists", "SplitBrain"}]
    /\ retryOf \in [FlushIds -> FlushIds \cup {NoReq}]
    /\ tombstoned \subseteq FlushIds
    /\ superseded \subseteq FlushIds
    /\ tokenResult \in [FlushIds -> [outcome: {"none", "served", "tombstoned",
                                              "superseded", "unsatisfiable"},
                                    present: BOOLEAN, tomb: BOOLEAN,
                                    sup: BOOLEAN]]
    /\ publishedAt \in [FlushIds -> 0..(MaxTicks + 1)]
    /\ queried \in BOOLEAN
    /\ queryAnswer \subseteq FlushIds

Init ==
    /\ Store!StoreInit
    /\ phase = [f \in FlushIds |-> "idle"]
    /\ pinned = [f \in FlushIds |-> NoC]
    /\ openedAt = [f \in FlushIds |-> 0]
    /\ retries = [f \in FlushIds |-> 0]
    /\ clock = 0
    /\ shardDead = [s \in Shards |-> FALSE]
    /\ ackKind = [f \in FlushIds |-> "none"]
    /\ marker = "absent"
    /\ lastPut = NoPut
    /\ retryOf = [f \in FlushIds |-> NoReq]
    /\ tombstoned = {}
    /\ superseded = {}
    /\ tokenResult = [f \in FlushIds |->
                        [outcome |-> "none", present |-> FALSE,
                         tomb |-> FALSE, sup |-> FALSE]]
    /\ publishedAt = [f \in FlushIds |-> MaxTicks + 1]
    /\ queried = FALSE
    /\ queryAnswer = {}

\* --- derived views ----------------------------------------------------------

\* A commit is query-visible exactly when its commit record is present.
Visible(f) == Store!Present(CommitKey(f))
DataPresent(f) == Store!Present(DataKey(f))

\* Every flush of a request that reached a durable commit.
DurableSet == {f \in FlushIds : Visible(f)}

\* The request is fully durable when every shard's flush committed.
AllShardsDurable ==
    \A s \in Shards : \E w \in Writers : Visible(<<w, s>>)

Expired(f) == clock > openedAt[f] + FlushLifetime

\* RetryDedups is a MUTATION, not shipped behaviour: a hypothetical
\* exactly-once mechanism for logs and spans. It suppresses the commit WRITE,
\* not the resend decision, and it guards every path that can write a commit
\* record. Both matter. A check made before either write cannot exclude the
\* other, because both attempts pass it; and guarding only the ordinary PUT
\* leaves the lost-response PUT free to create the second record. Actions are
\* atomic, so whichever write runs first disables the other.
DedupSuppressed(f) ==
    RetryDedups /\ \E h \in FlushIds :
        /\ \/ retryOf[f] = h
           \/ retryOf[h] = f
        /\ Store!Present(CommitKey(h))

\* ---------------------------------------------------------------------------
\* Writer actions. Each names the Rust symbol that performs the transition.
\* ---------------------------------------------------------------------------

\* ShardActor::flush_tenant. The identity and the bytes are pinned here, before
\* any store call, and every later retry reuses them.
PinFlush(f, c) ==
    /\ phase[f] = "idle"
    /\ ~shardDead[f[2]]
    /\ clock < MaxTicks
    /\ phase' = [phase EXCEPT ![f] = "pinned"]
    /\ pinned' = [pinned EXCEPT ![f] = c]
    /\ openedAt' = [openedAt EXCEPT ![f] = clock]
    /\ publishedAt' = [publishedAt EXCEPT ![f] = MaxTicks + 1]
    \* BROKEN switch: acknowledge in STRICT mode at enqueue, before anything
    \* is durable. Buffered mode's own early ack is a separate, correct
    \* action (BufferedAck) and is not what this switch models.
    /\ ackKind' = [ackKind EXCEPT ![f] =
                     IF AckAtEnqueue THEN "strict" ELSE @]
    /\ UNCHANGED <<retries, clock, shardDead, marker, lastPut, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* IngestRouter::write_points returning at enqueue in buffered mode: the client
\* is told before anything is durable, which is what buffered means.
BufferedAck(f) ==
    /\ phase[f] = "pinned"
    /\ ackKind[f] = "none"
    /\ ackKind' = [ackKind EXCEPT ![f] = "buffered"]
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* FlushCtx::put_data_object_with_retry -> publish::put_data_object.
\* PutMode::CreateIfAbsent; AlreadyExists is treated as success with NO
\* read-back, which is the assumption this model states rather than checks.
PutData(f) ==
    /\ phase[f] = "pinned"
    /\ ~Expired(f)
    /\ ~shardDead[f[2]]
    /\ Store!PutCreateIfAbsent(DataKey(f), pinned[f])
    /\ phase' = [phase EXCEPT ![f] = "data"]
    /\ UNCHANGED <<pinned, openedAt, retries, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* The response to the data PUT was lost: the effect landed, the caller saw a
\* failure and retries the same pinned flush. The retry is the PutData step
\* above, which finds its own object present and succeeds.
PutDataLostResponse(f) ==
    /\ phase[f] = "pinned"
    /\ ~Expired(f)
    /\ ~shardDead[f[2]]
    /\ retries[f] < MaxRetries
    /\ Store!PutCreateIfAbsentLostResponse(DataKey(f), pinned[f])
    /\ retries' = [retries EXCEPT ![f] = @ + 1]
    /\ UNCHANGED <<phase, pinned, openedAt, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* FlushCtx::publish_with_retry -> publish::publish_with_rng.
\* PutMode::CreateIfAbsent. The client does not pre-classify the outcome: it
\* calls the operator and records what the store held, so the witness can be
\* compared against the store rather than against the action's own belief.
PutCommit(f) ==
    /\ \/ phase[f] = "data"
       \/ (CommitBeforeData /\ phase[f] = "pinned")   \* BROKEN switch
    /\ ~DedupSuppressed(f)
    /\ ~Expired(f)
    /\ ~shardDead[f[2]]
    /\ LET k == CommitKey(f)
           existed == Store!Present(k)
           held == Store!ContentOf(k)
           same == held = pinned[f]
           \* resolve_already_exists: on AlreadyExists the writer GETs the
           \* winner and compares content hashes. Equal is idempotent success;
           \* different is PublishError::SplitBrain, which panics the shard
           \* actor, so every later write to that shard fails ShardUnavailable.
           split == existed /\ ~same /\ ~SkipHashCompare
       IN /\ Store!PutCreateIfAbsent(k, pinned[f])
          /\ lastPut' = [kind |-> "commit", flush |-> f, mine |-> pinned[f],
                         stored |-> IF existed THEN held ELSE pinned[f],
                         outcome |-> IF split THEN "SplitBrain"
                                     ELSE IF existed THEN "AlreadyExists"
                                     ELSE "Ok"]
          /\ phase' = [phase EXCEPT ![f] = IF split THEN "stopped"
                                           ELSE "committed"]
          /\ shardDead' = IF split THEN [shardDead EXCEPT ![f[2]] = TRUE]
                          ELSE shardDead
          /\ publishedAt' = [publishedAt EXCEPT ![f] =
                                IF Store!Present(k) THEN @ ELSE clock]
    /\ UNCHANGED <<pinned, openedAt, retries, clock, ackKind, marker, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* The commit PUT landed and its response was lost. The caller retries the same
\* pinned flush; that retry takes the AlreadyExists path above.
PutCommitLostResponse(f) ==
    /\ phase[f] = "data"
    /\ ~DedupSuppressed(f)
    /\ ~Expired(f)
    /\ ~shardDead[f[2]]
    /\ retries[f] < MaxRetries
    /\ publishedAt' = [publishedAt EXCEPT ![f] =
                        IF Store!Present(CommitKey(f)) THEN @ ELSE clock]
    /\ Store!PutCreateIfAbsentLostResponse(CommitKey(f), pinned[f])
    /\ retries' = [retries EXCEPT ![f] = @ + 1]
    /\ UNCHANGED <<phase, pinned, openedAt, clock, shardDead, ackKind, marker, lastPut, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* A transient store failure applies nothing; the caller retries.
TransientFailure(f) ==
    /\ phase[f] \in {"pinned", "data"}
    /\ ~Expired(f)
    /\ retries[f] < MaxRetries
    /\ Store!TransientFailure
    /\ retries' = [retries EXCEPT ![f] = @ + 1]
    /\ UNCHANGED <<phase, pinned, openedAt, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* FlushCtx::ack_waiters, then the router's token collection and
\* otlp_http::otlp_response. Strict mode acknowledges only after the commit
\* record is durable.
StrictAck(f) ==
    /\ phase[f] = "committed"
    /\ ackKind[f] \in {"none", "buffered"}
    /\ phase' = [phase EXCEPT ![f] = "acked"]
    /\ ackKind' = [ackKind EXCEPT ![f] = "strict"]
    /\ UNCHANGED <<pinned, openedAt, retries, clock, shardDead, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* WriteError::AckTimeout. The client's wait is dropped while the flush task
\* keeps running: it may still commit, and no one observes its token.
AckTimeout(f) ==
    /\ phase[f] \in {"pinned", "data"}
    /\ ackKind[f] = "none"
    /\ ackKind' = [ackKind EXCEPT ![f] = "timeout"]
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* WriteError::Abandoned. FlushCtx::bound_to_deadline races every attempt
\* against flush_open + max_flush_lifetime; nothing is published after it.
Abandon(f) ==
    /\ phase[f] \in {"pinned", "data"}
    /\ Expired(f)
    /\ phase' = [phase EXCEPT ![f] = "abandoned"]
    /\ ackKind' = [ackKind EXCEPT ![f] = IF @ = "none" THEN "error" ELSE @]
    /\ UNCHANGED <<pinned, openedAt, retries, clock, shardDead, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* A crash loses everything in memory. Durable objects stay; the pinned flush
\* is gone and its client, if it was still waiting, gets an error. The identity
\* is RETIRED rather than reused: the restarted process mints a fresh writer id
\* (IngestRouter::with_rng's per-generation factory) and seq is monotonic per
\* (writer_id, epoch, shard), so the same commit key is never pinned twice by
\* design. A different flush id in this model is that restarted process.
Crash(f) ==
    /\ phase[f] \in {"pinned", "data", "committed"}
    /\ phase' = [phase EXCEPT ![f] = "retired"]
    /\ pinned' = [pinned EXCEPT ![f] = @]
    /\ retries' = [retries EXCEPT ![f] = 0]
    /\ ackKind' = [ackKind EXCEPT ![f] = IF @ = "none" THEN "error" ELSE @]
    /\ UNCHANGED <<openedAt, clock, shardDead, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* ravel_ingest::write_marker, from the logs and traces handlers. The marker is
\* PUT CreateIfAbsent only after every shard's commit is durable and only for a
\* fully successful request.
WriteMarker ==
    /\ MarkersApply
    /\ marker = "absent"
    /\ \/ AllShardsDurable
       \/ (MarkerAfterFirstShard /\ DurableSet # {})   \* BROKEN switch
    /\ Store!PutCreateIfAbsent(MarkerKey, CHOOSE c \in Contents : TRUE)
    /\ marker' = "written"
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, ackKind, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>

\* Accidental reuse of a commit identity with different content: the hazard
\* ADR-0002 names. The protocol does not prevent it, it DETECTS it: the commit
\* PUT finds the key present, reads the winner back and compares content hashes
\* (publish::resolve_already_exists), and a mismatch is PublishError::SplitBrain.
ReuseIdentity(f, c) ==
    /\ phase[f] = "retired"
    /\ c # pinned[f]
    /\ ~shardDead[f[2]]
    /\ clock < MaxTicks
    /\ phase' = [phase EXCEPT ![f] = "pinned"]
    /\ pinned' = [pinned EXCEPT ![f] = c]
    /\ openedAt' = [openedAt EXCEPT ![f] = clock]
    /\ retries' = [retries EXCEPT ![f] = 0]
    /\ publishedAt' = [publishedAt EXCEPT ![f] = MaxTicks + 1]
    /\ UNCHANGED <<clock, shardDead, ackKind, marker, lastPut, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* A CLIENT retry after a lost acknowledgement. The first attempt's client saw
\* a timeout or an error, so it resends the same payload; the gateway routes it
\* to a fresh flush identity (`g`) carrying the SAME content. This is the
\* transition that produces at-least-once delivery for logs and spans.
\*
\* RetryDedups is the BROKEN switch: it makes the retry consult the store for
\* an existing record of the same content and skip publishing, which is
\* exactly the exactly-once behaviour Ravel does not offer. It suppresses the
\* WRITE, not a counter, so the obligation below is derived from the store.
ClientRetry(f, g) ==
    /\ f # g
    \* A resend carries the same series, so the router picks the same shard.
    \* The resend is therefore a different writer's attempt on that shard,
    \* which is why the duplicate configurations use two writers and one
    \* shard and every other configuration leaves this action disabled.
    /\ g[2] = f[2]
    \* One client resend per behaviour. A second resend adds interleavings
    \* without adding an outcome: the duplicate obligation and the dedup
    \* mutant are both settled by the first one.
    /\ \A h \in FlushIds : retryOf[h] = NoReq
    /\ ackKind[f] \in {"timeout", "error"}
    /\ pinned[f] # NoC
    /\ phase[g] = "idle"
    /\ ~shardDead[g[2]]
    /\ clock < MaxTicks
    /\ ~(MarkersApply /\ marker = "written")   \* a usable marker replays instead
    /\ phase' = [phase EXCEPT ![g] = "pinned"]
    /\ pinned' = [pinned EXCEPT ![g] = pinned[f]]
    /\ openedAt' = [openedAt EXCEPT ![g] = clock]
    /\ publishedAt' = [publishedAt EXCEPT ![g] = MaxTicks + 1]
    \* g is the client's resend of f, which is what makes a second durable
    \* record a duplicate rather than an unrelated write of equal content
    /\ retryOf' = [retryOf EXCEPT ![g] = f]
    /\ UNCHANGED <<retries, clock, shardDead, ackKind, marker, lastPut, tombstoned, superseded, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* Retention tombstones a bucket (retention::write_tombstone), and a compaction
\* or erasure rewrite supersedes a record. Both are modelled only as far as a
\* commit-token query must distinguish them.
\* At most one bucket is retired per behaviour: the token invariant needs one
\* retired bucket to distinguish its outcomes, and letting every flush be
\* retired independently multiplies the state space without adding coverage.
TombstoneBucket(f) ==
    /\ Visible(f)
    /\ tombstoned = {}
    /\ superseded = {}
    /\ f \notin tombstoned
    /\ tombstoned' = tombstoned \cup {f}
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, ackKind, marker, lastPut, publishedAt, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

SupersedeRecord(f) ==
    /\ Visible(f)
    /\ tombstoned = {}
    /\ superseded = {}
    /\ f \notin superseded
    /\ f \notin tombstoned
    /\ superseded' = superseded \cup {f}
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* Catalog::resolve_min_token: an exact-key GET on the commit key the token
\* names, with the bucket listing behind it. The four outcomes are the shipped
\* ones and each is decided from the STORE plus the tombstone and supersession
\* state, never from what the writer believed.
ResolveToken(f) ==
    \* One token query per behaviour, for the same reason.
    /\ \A g \in FlushIds : tokenResult[g].outcome = "none"
    /\ pinned[f] # NoC
    /\ tokenResult' = [tokenResult EXCEPT ![f] =
            [outcome |-> IF Store!Present(CommitKey(f)) THEN "served"
                         ELSE IF f \in tombstoned THEN "tombstoned"
                         ELSE IF f \in superseded THEN "superseded"
                         ELSE "unsatisfiable",
             present |-> Store!Present(CommitKey(f)),
             tomb    |-> f \in tombstoned,
             sup     |-> f \in superseded]]
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, queried, queryAnswer>>
    /\ UNCHANGED sVars

\* The read path, abstracted to what an answer contains rather than how it was
\* computed. One run per behaviour, like the other single-fire query actions:
\* the interleavings that matter are what the store held when it ran, not how
\* many times it is asked. Gated on CheckQuery: even at one firing, WHEN it
\* fires is itself a choice TLC explores at every reachable state, so a cfg
\* that does not check NoUncommittedDataVisible turns the action off outright
\* rather than pay for that choice.
RunQuery ==
    /\ CheckQuery
    /\ ~queried
    /\ queried' = TRUE
    /\ queryAnswer' = {f \in FlushIds :
                          IF QueryReadsDataDirectly
                          THEN Visible(f) \/ DataPresent(f)   \* BROKEN switch
                          ELSE Visible(f)}
    /\ UNCHANGED <<phase, pinned, openedAt, retries, clock, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult>>
    /\ UNCHANGED sVars

Tick ==
    /\ clock < MaxTicks
    /\ clock' = clock + 1
    /\ UNCHANGED <<phase, pinned, openedAt, retries, shardDead, ackKind, marker, lastPut, publishedAt, tombstoned, superseded, retryOf, tokenResult, queried, queryAnswer>>
    /\ UNCHANGED sVars

Next ==
    \/ \E f \in FlushIds, c \in Contents : PinFlush(f, c)
    \/ \E f \in FlushIds : BufferedAck(f)
    \/ \E f \in FlushIds : PutData(f)
    \/ \E f \in FlushIds : PutDataLostResponse(f)
    \/ \E f \in FlushIds : PutCommit(f)
    \/ \E f \in FlushIds : PutCommitLostResponse(f)
    \/ \E f \in FlushIds : TransientFailure(f)
    \/ \E f \in FlushIds : StrictAck(f)
    \/ \E f \in FlushIds : AckTimeout(f)
    \/ \E f \in FlushIds : Abandon(f)
    \/ \E f \in FlushIds : Crash(f)
    \/ \E f \in FlushIds, c \in Contents : ReuseIdentity(f, c)
    \/ \E f, g \in FlushIds : ClientRetry(f, g)
    \/ \E f \in FlushIds : TombstoneBucket(f)
    \/ \E f \in FlushIds : SupersedeRecord(f)
    \/ \E f \in FlushIds : ResolveToken(f)
    \/ WriteMarker
    \/ RunQuery
    \/ Tick

\* Legitimate terminal states exist: once the clock reaches MaxTicks and every
\* flush has settled, no action is enabled. That is the model running out of
\* bounded time, not a protocol deadlock, so every cfg sets CHECK_DEADLOCK
\* FALSE and this predicate says which states are meant to be terminal.
Terminal ==
    /\ clock = MaxTicks
    /\ \A f \in FlushIds :
          phase[f] \in {"idle", "acked", "abandoned", "stopped", "retired"}

Spec == Init /\ [][Next]_vars

\* Fairness is asserted only on the actions the implementation justifies: the
\* store retry loop makes progress (FlushCtx's bounded retry with backoff) and
\* the flush task runs to its next durable step. Nothing is fair about crashes,
\* timeouts or the clock. Safety never depends on any of this.
Fairness ==
    /\ \A f \in FlushIds : WF_vars(PutData(f))
    /\ \A f \in FlushIds : WF_vars(PutCommit(f))
    /\ \A f \in FlushIds : WF_vars(StrictAck(f))

FairSpec == Spec /\ Fairness

\* ---------------------------------------------------------------------------
\* Safety invariants. Every one reads the store, or the witness of what a store
\* operation returned, never a field an action wrote about itself.
\* ---------------------------------------------------------------------------

\* A commit record never exists without its data object.
NoCommitWithoutData ==
    \A f \in FlushIds : Visible(f) => DataPresent(f)

\* Nothing is query-visible before its commit record is durable: every flush
\* RunQuery placed in its answer has a durable commit record. Checked against
\* queryAnswer, the read path's own output, never against Visible or
\* DurableSet directly, both of which are defined FROM the store: an
\* invariant stated over them alone would hold by construction regardless of
\* what the read path actually returns.
NoUncommittedDataVisible ==
    \A f \in queryAnswer : Visible(f)

\* A strict acknowledgement implies both objects are durable.
StrictAckImpliesDurable ==
    \A f \in FlushIds :
        ackKind[f] = "strict" => (DataPresent(f) /\ Visible(f))

\* A commit identity is never bound to two different contents: whenever the
\* commit PUT found the key present, the stored content equalled the caller's,
\* or the writer stopped the shard.
OneIdentityOneContent ==
    (lastPut.kind = "commit" /\ lastPut.outcome = "AlreadyExists")
        => lastPut.stored = lastPut.mine

\* Split brain stops the shard rather than publishing over the winner.
SplitBrainStopsTheShard ==
    (lastPut.kind = "commit" /\ lastPut.outcome = "SplitBrain")
        => (shardDead[lastPut.flush[2]] /\ phase[lastPut.flush] = "stopped")

\* Retrying the same pinned flush is idempotent: the record a retry finds is
\* the one it would have written.
RetrySamePinnedFlushIdempotent ==
    \A f \in FlushIds :
        phase[f] \in {"committed", "acked"}
            => Store!ContentOf(CommitKey(f)) = pinned[f]

\* Nothing is published after the flush lifetime elapsed. The claim is about
\* the ABANDONED ATTEMPT, not about the key: an identity that committed in an
\* earlier life and was then reused keeps that earlier record, which is
\* correct. So the invariant is that the abandoned attempt's own pinned
\* content is not what the record holds.
\* No store write is issued after the deadline. A commit whose response was
\* LOST is durable even though its writer then gives up and reports an error:
\* that is the documented ambiguity, not a violation, so the claim is about
\* WHEN the write landed, which the publishedAt witness records at the moment
\* the store operation ran.
NoPublishAfterAbandon ==
    \A f \in FlushIds :
        publishedAt[f] =< MaxTicks
            => publishedAt[f] =< openedAt[f] + FlushLifetime

\* The marker is written only when every shard of the request is durable.
MarkerImpliesAllShardsDurable ==
    (marker = "written") => AllShardsDurable

\* A commit-token query includes the named commit, reports the bucket
\* deliberately retired (tombstoned, or served through the record that
\* superseded it), or fails explicitly. It never silently serves a stale
\* result: every outcome is checked against the store, not against what the
\* writer believed. (Broken by a resolution that answers "served" for an
\* absent record, or "unsatisfiable" for a present one.)
TokenNeverServesStale ==
    \A f \in FlushIds :
        LET r == tokenResult[f] IN
        /\ (r.outcome = "served"        => r.present)
        /\ (r.outcome = "unsatisfiable" => ~r.present /\ ~r.tomb /\ ~r.sup)
        /\ (r.outcome = "tombstoned"    => r.tomb /\ ~r.present)
        /\ (r.outcome = "superseded"    => r.sup /\ ~r.present)

\* Every signal reports the durable tokens of a partial multi-shard commit:
\* IngestRouter::write_points, LogIngestRouter::await_strict_acks and
\* SpanIngestRouter::write each carry a PartialWrite{durable} variant now, so
\* no signal is exempt from a strict ack implying its own record is durable.
\* This used to hold only `~ReportsPartial` (Signal = "logs" was the
\* exception); that asymmetry is retired, not current behaviour, so the
\* guard is gone rather than restated.
PartialReportingMatchesSignal ==
    \A f \in FlushIds : ackKind[f] = "strict" => Visible(f)

===============================================================================

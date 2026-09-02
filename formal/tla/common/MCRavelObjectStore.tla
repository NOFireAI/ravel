-------------------------- MODULE MCRavelObjectStore --------------------------
(*****************************************************************************)
(* Self-test model for RavelObjectStore.tla (ADR-1113 D2, task T1).          *)
(*                                                                           *)
(* Two clients issue the store operators against a small key set. Each        *)
(* mutating client action calls the store operator and records, in a single   *)
(* witness `lastOp`, the caller-visible outcome the result function computed   *)
(* alongside the store record before and after the step. The invariants read   *)
(* that witness and the store, never a switch or a bookkeeping ghost that      *)
(* merely restates the switch: an effect that disagrees with its outcome (a    *)
(* CAS applied on a stale version, a lost response that never landed, a        *)
(* transient failure that changed the store) is what a violation reports.      *)
(*                                                                           *)
(* Negative-control switches each break one clause so TLC must reject the       *)
(* broken variant; all default FALSE (the correct model) and a negative/<name>  *)
(* .cfg flips exactly one. LostResponseDropsEffect and CasAcceptsStale break a   *)
(* safety invariant (exit 12); ListStalls disables the fair list action so the   *)
(* ListEventuallyComplete liveness property fails (exit 13).                  *)
(*                                                                           *)
(* Bounding: `opCount` caps the number of mutating operations (MaxOps) so the  *)
(* monotonic version counter stays finite. Listing and multipart              *)
(* begin/part/abort are not charged against the budget, so a traversal can     *)
(* always run to completion (needed for ListEventuallyComplete).             *)
(*                                                                           *)
(* Ghost/witness variables (not part of the store):                          *)
(*   opCount        budget on mutating operations                            *)
(*   createWins[k]  successful CreateIfAbsent wins in the current presence    *)
(*                  interval (reset on delete)                               *)
(*   lastWritten[k] the content the latest durable write claims for k         *)
(*                  (NoWrite when k has no claimed content)                   *)
(*   lastOp         the most recent mutating operation: kind, key, outcome,   *)
(*                  the version a CAS used, the claimed content, and the store *)
(*                  record before and after (checked in the post-state)       *)
(*   mpBegin[u]     the store record of u's upload key captured at begin       *)
(*   dedupSet       a listing consumer that deduplicates deliveries           *)
(*   deliveryCount  a listing consumer that counts every delivery             *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Keys, Content, NoContent, Clients,
    MaxOps,                  \* budget on mutating operations
    NoWrite,                 \* lastWritten sentinel: no claimed content
    LostResponseDropsEffect, \* negative: a lost-response write drops its effect
    CasAcceptsStale,         \* negative: a CAS accepts a stale/absent version
    ListStalls               \* negative (liveness): the list action never progresses

ASSUME MaxOps \in Nat
ASSUME NoWrite \notin Content

VARIABLES store, lastModified, versionCounter, uploads, listState,
          opCount, createWins, lastWritten, lastOp, mpBegin, dedupSet, deliveryCount

INSTANCE RavelObjectStore

mcStoreVars == <<store, lastModified, versionCounter, uploads, listState>>
opGhosts    == <<opCount, createWins, lastWritten, lastOp, mpBegin>>
listGhosts  == <<dedupSet, deliveryCount>>
vars == <<store, lastModified, versionCounter, uploads, listState,
          opCount, createWins, lastWritten, lastOp, mpBegin, dedupSet, deliveryCount>>

Outcomes == {"Ok", "AlreadyExists", "PreconditionFailed", "Failure"}
OpKinds  == {"none", "create", "overwrite", "cas", "delete",
             "transient", "lostOverwrite", "mpcomplete"}
RecType  == [present: BOOLEAN, content: Content, version: 0..MaxOps]

\* No plain write or new upload may target a key that has an active upload, so
\* the multipart-visibility invariant can compare the target key against the
\* record captured at begin without a concurrent overwrite confounding it.
NoActiveUploadOn(k) == \A u \in Clients : ~(uploads[u].active /\ uploads[u].key = k)

MCInit ==
    /\ StoreInit
    /\ opCount = 0
    /\ createWins = [k \in Keys |-> 0]
    /\ lastWritten = [k \in Keys |-> NoWrite]
    /\ lastOp = [kind |-> "none", key |-> CHOOSE k \in Keys : TRUE,
                 outcome |-> "Ok", versionUsed |-> 0,
                 before |-> EmptyRec, after |-> EmptyRec, content |-> NoContent]
    /\ mpBegin = [u \in Clients |-> EmptyRec]
    /\ dedupSet = {}
    /\ deliveryCount = 0

\* --- Mutating client actions (charged against the budget) -------------------

DoCreate(k, c) ==
    /\ opCount < MaxOps
    /\ NoActiveUploadOn(k)
    /\ LET ok == ~store[k].present IN
        /\ PutCreateIfAbsent(k, c)
        /\ createWins' = [createWins EXCEPT ![k] = IF ok THEN @ + 1 ELSE @]
        /\ lastWritten' = [lastWritten EXCEPT ![k] = IF ok THEN c ELSE @]
    /\ lastOp' = [kind |-> "create", key |-> k, outcome |-> CreateResult(k),
                  versionUsed |-> 0, before |-> store[k], after |-> store'[k],
                  content |-> c]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<mpBegin>>
    /\ UNCHANGED listGhosts

DoOverwrite(k, c) ==
    /\ opCount < MaxOps
    /\ NoActiveUploadOn(k)
    /\ PutOverwrite(k, c)
    /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
    /\ lastOp' = [kind |-> "overwrite", key |-> k, outcome |-> OverwriteResult,
                  versionUsed |-> 0, before |-> store[k], after |-> store'[k],
                  content |-> c]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<createWins, mpBegin>>
    /\ UNCHANGED listGhosts

\* The client does NOT pre-classify freshness; it calls the operator, which
\* decides the effect from its own precondition, and records the outcome the
\* result function computes. The CasAcceptsStale switch is the only thing that
\* overrides the operator, and the witness catches the resulting mismatch.
DoCas(k, v, c) ==
    /\ opCount < MaxOps
    /\ NoActiveUploadOn(k)
    /\ IF CasAcceptsStale /\ CasResult(k, v) # "Ok"
           THEN PutOverwrite(k, c)         \* BROKEN switch: apply despite precondition
           ELSE PutCasVersion(k, v, c)     \* correct: the operator decides
    /\ LET applied == CasAcceptsStale \/ CasResult(k, v) = "Ok" IN
        lastWritten' = IF applied THEN [lastWritten EXCEPT ![k] = c] ELSE lastWritten
    /\ lastOp' = [kind |-> "cas", key |-> k, outcome |-> CasResult(k, v),
                  versionUsed |-> v, before |-> store[k], after |-> store'[k],
                  content |-> c]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<createWins, mpBegin>>
    /\ UNCHANGED listGhosts

DoDelete(k) ==
    /\ opCount < MaxOps
    /\ NoActiveUploadOn(k)
    /\ Delete(k)
    /\ createWins' = [createWins EXCEPT ![k] = 0]
    /\ lastWritten' = [lastWritten EXCEPT ![k] = NoWrite]
    /\ lastOp' = [kind |-> "delete", key |-> k, outcome |-> DeleteResult,
                  versionUsed |-> 0, before |-> store[k], after |-> store'[k],
                  content |-> NoContent]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<mpBegin>>
    /\ UNCHANGED listGhosts

\* TransientFailure: applies nothing, caller observes a retryable Failure.
DoTransient ==
    /\ opCount < MaxOps
    /\ \E k \in Keys :
        /\ TransientFailure
        /\ lastOp' = [kind |-> "transient", key |-> k, outcome |-> "Failure",
                      versionUsed |-> 0, before |-> store[k], after |-> store'[k],
                      content |-> NoContent]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<createWins, lastWritten, mpBegin>>
    /\ UNCHANGED listGhosts

\* LostResponse write: the effect is applied and the caller observes Failure (an
\* ack loss). The client's write is durable, so lastWritten records it. The
\* negative switch drops the effect, which must break both the lost-response
\* invariant and ReadAfterWrite.
DoLostOverwrite(k, c) ==
    /\ opCount < MaxOps
    /\ NoActiveUploadOn(k)
    /\ IF LostResponseDropsEffect
           THEN UNCHANGED mcStoreVars            \* BROKEN: caller retries a lost effect
           ELSE PutOverwriteLostResponse(k, c)   \* correct: effect applied, response lost
    /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
    /\ lastOp' = [kind |-> "lostOverwrite", key |-> k, outcome |-> "Failure",
                  versionUsed |-> 0, before |-> store[k], after |-> store'[k],
                  content |-> c]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<createWins, mpBegin>>
    /\ UNCHANGED listGhosts

\* --- Multipart client actions -----------------------------------------------

DoMultipartBegin(u, k) ==
    /\ NoActiveUploadOn(k)
    /\ MultipartBegin(u, k)
    /\ mpBegin' = [mpBegin EXCEPT ![u] = store[k]]
    /\ UNCHANGED <<opCount, createWins, lastWritten, lastOp>>
    /\ UNCHANGED listGhosts

DoMultipartPart(u, c) ==
    /\ MultipartPart(u, c)
    /\ UNCHANGED opGhosts
    /\ UNCHANGED listGhosts

DoMultipartComplete(u) ==
    /\ opCount < MaxOps
    /\ MultipartComplete(u)
    /\ lastWritten' = [lastWritten EXCEPT ![uploads[u].key] = uploads[u].content]
    /\ lastOp' = [kind |-> "mpcomplete", key |-> uploads[u].key,
                  outcome |-> OverwriteResult, versionUsed |-> 0,
                  before |-> store[uploads[u].key], after |-> store'[uploads[u].key],
                  content |-> uploads[u].content]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<createWins, mpBegin>>
    /\ UNCHANGED listGhosts

DoMultipartAbort(u) ==
    /\ MultipartAbort(u)
    /\ UNCHANGED opGhosts
    /\ UNCHANGED listGhosts

\* --- Listing client actions (not charged against the budget) -----------------
\* Both the deduplicating and the counting consumer advance on every delivery.
\* DoListProgress delivers an undelivered snapshot key; that transition is a
\* subset of DoListReturn's, so WF over it is sound though it is not a separate
\* Next disjunct.

DeliverGhosts(k) ==
    /\ dedupSet' = dedupSet \cup {k}
    /\ deliveryCount' = deliveryCount + 1

DoListBegin ==
    /\ ListBegin
    /\ dedupSet' = {}
    /\ deliveryCount' = 0
    /\ UNCHANGED opGhosts

DoListReturn(k) ==
    /\ ListReturn(k)
    /\ DeliverGhosts(k)
    /\ UNCHANGED opGhosts

DoListProgress(k) ==
    /\ ~ListStalls
    /\ ListProgress(k)
    /\ DeliverGhosts(k)
    /\ UNCHANGED opGhosts

DoListEnd ==
    /\ ListEnd
    /\ dedupSet' = {}
    /\ deliveryCount' = 0
    /\ UNCHANGED opGhosts

\* --- Next / specs -----------------------------------------------------------

MCNext ==
    \/ \E k \in Keys, c \in RealContent : DoCreate(k, c)
    \/ \E k \in Keys, c \in RealContent : DoOverwrite(k, c)
    \/ \E k \in Keys, v \in 0..MaxOps, c \in RealContent : DoCas(k, v, c)
    \/ \E k \in Keys : DoDelete(k)
    \/ DoTransient
    \/ \E k \in Keys, c \in RealContent : DoLostOverwrite(k, c)
    \/ \E u \in Clients, k \in Keys : DoMultipartBegin(u, k)
    \/ \E u \in Clients, c \in RealContent : DoMultipartPart(u, c)
    \/ \E u \in Clients : DoMultipartComplete(u)
    \/ \E u \in Clients : DoMultipartAbort(u)
    \/ DoListBegin
    \/ \E k \in Keys : DoListReturn(k)
    \/ DoListEnd

MCListProgress == \E k \in Keys : DoListProgress(k)

MCSpec == MCInit /\ [][MCNext]_vars
FairSpec == MCSpec /\ WF_vars(MCListProgress)

Symmetry == Permutations(Clients)

\* --- Invariants (the module's own semantics, read off the store + witness) ---

MCTypeOK ==
    /\ StoreTypeOK
    /\ versionCounter \in 0..MaxOps
    /\ \A k \in Keys : store[k].version \in 0..MaxOps
    /\ opCount \in 0..MaxOps
    /\ createWins \in [Keys -> 0..MaxOps]
    /\ lastWritten \in [Keys -> Content \cup {NoWrite}]
    /\ lastOp \in [kind: OpKinds, key: Keys, outcome: Outcomes,
                   versionUsed: 0..MaxOps, before: RecType, after: RecType,
                   content: Content]
    /\ mpBegin \in [Clients -> RecType]
    /\ dedupSet \subseteq Keys
    /\ deliveryCount \in 0..(Cardinality(Keys) * MaxListMultiplicity)

\* At most one CreateIfAbsent wins per presence interval (a second create on a
\* present key gets AlreadyExists; a delete resets the interval).
CreateIfAbsentWinnerUnique == \A k \in Keys : createWins[k] <= 1

\* A create's outcome matches its store delta: an Ok create made an absent key
\* present with the claimed content; an AlreadyExists create changed nothing.
CreateOutcomeMatchesEffect ==
    lastOp.kind = "create" =>
        /\ (lastOp.outcome = "Ok" =>
                /\ ~lastOp.before.present
                /\ lastOp.after.present
                /\ lastOp.after.content = lastOp.content)
        /\ (lastOp.outcome = "AlreadyExists" =>
                /\ lastOp.before.present
                /\ lastOp.after = lastOp.before)

\* A successful CAS used the current version of a present key; a CAS that did not
\* succeed left the key's store record unchanged. (Broken by CasAcceptsStale and
\* by deleting the absent-key disjunct from PutCasVersion.)
CasOutcomeMatchesEffect ==
    lastOp.kind = "cas" =>
        /\ (lastOp.outcome = "Ok" =>
                /\ lastOp.before.present
                /\ lastOp.versionUsed = lastOp.before.version
                /\ lastOp.after.present
                /\ lastOp.after.content = lastOp.content)
        /\ (lastOp.outcome # "Ok" => lastOp.after = lastOp.before)

\* Read-after-write: the latest durable write for a present key is visible.
\* (Broken by LostResponseDropsEffect, which claims a write that never landed.)
ReadAfterWrite ==
    \A k \in Keys :
        lastWritten[k] # NoWrite => (store[k].present /\ store[k].content = lastWritten[k])

\* A lost-response write applied its effect even though the caller saw Failure.
\* (Broken by LostResponseDropsEffect.)
LostResponseEffectApplied ==
    lastOp.kind = "lostOverwrite" =>
        /\ lastOp.outcome = "Failure"
        /\ lastOp.after.present
        /\ lastOp.after.content = lastOp.content

\* A transient failure applied nothing and the caller saw Failure.
TransientLeavesNothing ==
    lastOp.kind = "transient" =>
        /\ lastOp.outcome = "Failure"
        /\ lastOp.after = lastOp.before

\* Deletion is idempotent and total: after a delete the key is absent and the
\* outcome is Ok; a delete of an absent key changed no observable state.
DeleteIdempotent ==
    lastOp.kind = "delete" =>
        /\ lastOp.outcome = "Ok"
        /\ ~lastOp.after.present
        /\ (~lastOp.before.present => lastOp.after = lastOp.before)

\* Nothing from an in-progress multipart upload is published before complete:
\* the target key's store record equals the value captured at begin.
\* (Broken by a MultipartPart that writes into the store early.)
MultipartInvisibleUntilComplete ==
    \A u \in Clients : uploads[u].active => store[uploads[u].key] = mpBegin[u]

\* The deduplicating consumer tracks exactly the delivered support, and the
\* counting consumer never reports fewer than the distinct count: a duplicate
\* delivery (a distinct step now that deliveries are counted per key) shows up
\* as deliveryCount exceeding the deduplicated cardinality.
ListingConsumersConsistent ==
    /\ dedupSet = Delivered
    /\ deliveryCount >= Cardinality(dedupSet)

\* --- Liveness (checked against FairSpec only) -------------------------------

\* Every started listing eventually returns every key present when it started.
\* Fairness assumption: WF on MCListProgress (the list action) only.
ListEventuallyComplete ==
    listState.active ~> (listState.snapshot \subseteq Delivered)

=============================================================================

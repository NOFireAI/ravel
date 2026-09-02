-------------------------- MODULE MCRavelObjectStore --------------------------
(*****************************************************************************)
(* Self-test model for RavelObjectStore.tla (ADR-1113 D2, task T1).          *)
(*                                                                           *)
(* Two clients issue the store operators against a small key set. The        *)
(* invariants pin the module's OWN semantics; the negative-control switches   *)
(* (LostResponseDropsEffect, CasAcceptsStale) each break one clause so TLC    *)
(* must reject the broken variant. All switches default FALSE (the correct    *)
(* model); a negative/<name>.cfg flips exactly one.                          *)
(*                                                                           *)
(* Bounding: `opCount` caps the number of mutating operations (MaxOps) so    *)
(* the per-key version stays finite. Listing and multipart begin/part/abort   *)
(* are not charged against the budget, so a traversal can always run to       *)
(* completion (needed for the ListEventuallyComplete liveness property).      *)
(*                                                                           *)
(* Ghost variables (not part of the store; they record enough history to      *)
(* state the invariants):                                                     *)
(*   createWins[k]     successful CreateIfAbsent wins in the current          *)
(*                     presence interval (reset on delete)                    *)
(*   casStale          a CAS succeeded against a stale/absent version         *)
(*   lastWritten[k]    the content the latest durable write claims for k      *)
(*                     (NoWrite when k has no claimed content)                *)
(*   mpPublished[u]    whether u's current upload has been published          *)
(*   lastDeletedKey    the key of the most recent delete (NoKey otherwise)    *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Keys, Content, NoContent, Clients,
    MaxOps,                  \* budget on mutating operations
    NoWrite,                 \* lastWritten sentinel: no claimed content
    NoKey,                   \* lastDeletedKey sentinel
    LostResponseDropsEffect, \* negative: a lost-response write drops its effect
    CasAcceptsStale          \* negative: a CAS accepts a stale/absent version

ASSUME MaxOps \in Nat
ASSUME NoWrite \notin Content
ASSUME NoKey \notin Keys

VARIABLES store, lastModified, uploads, listState,
          opCount, createWins, casStale, lastWritten, mpPublished, lastDeletedKey

INSTANCE RavelObjectStore

mcStoreVars == <<store, lastModified, uploads, listState>>
ghostVars == <<opCount, createWins, casStale, lastWritten, mpPublished, lastDeletedKey>>
vars == <<store, lastModified, uploads, listState,
          opCount, createWins, casStale, lastWritten, mpPublished, lastDeletedKey>>

MCInit ==
    /\ StoreInit
    /\ opCount = 0
    /\ createWins = [k \in Keys |-> 0]
    /\ casStale = FALSE
    /\ lastWritten = [k \in Keys |-> NoWrite]
    /\ mpPublished = [u \in Clients |-> FALSE]
    /\ lastDeletedKey = NoKey

\* --- Mutating client actions (charged against the budget) -------------------

DoCreate(k, c) ==
    /\ opCount < MaxOps
    /\ LET ok == ~store[k].present IN
        /\ PutCreateIfAbsent(k, c)
        /\ createWins' = [createWins EXCEPT ![k] = IF ok THEN @ + 1 ELSE @]
        /\ lastWritten' = [lastWritten EXCEPT ![k] = IF ok THEN c ELSE @]
    /\ UNCHANGED <<casStale, mpPublished>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

DoOverwrite(k, c) ==
    /\ opCount < MaxOps
    /\ PutOverwrite(k, c)
    /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
    /\ UNCHANGED <<createWins, casStale, mpPublished>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

DoCas(k, v, c) ==
    /\ opCount < MaxOps
    /\ LET fresh == store[k].present /\ store[k].version = v IN
        IF fresh
            THEN /\ PutCasVersion(k, v, c)
                 /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
                 /\ UNCHANGED <<createWins, casStale, mpPublished>>
            ELSE IF CasAcceptsStale
                     THEN /\ PutOverwrite(k, c)         \* BROKEN: stale CAS applied
                          /\ casStale' = TRUE
                          /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
                          /\ UNCHANGED <<createWins, mpPublished>>
                     ELSE /\ PutCasVersion(k, v, c)     \* correct: PreconditionFailed, no-op
                          /\ UNCHANGED <<createWins, casStale, lastWritten, mpPublished>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

DoDelete(k) ==
    /\ opCount < MaxOps
    /\ Delete(k)
    /\ createWins' = [createWins EXCEPT ![k] = 0]
    /\ lastWritten' = [lastWritten EXCEPT ![k] = NoWrite]
    /\ UNCHANGED <<casStale, mpPublished>>
    /\ lastDeletedKey' = k
    /\ opCount' = opCount + 1

\* TransientFailure: applies nothing, caller observes a retryable error.
DoTransient ==
    /\ opCount < MaxOps
    /\ UNCHANGED mcStoreVars
    /\ UNCHANGED <<createWins, casStale, lastWritten, mpPublished>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

\* LostResponse write: the effect is applied and the caller observes failure
\* (an ack loss). The client's write is durable, so lastWritten records it.
\* The negative switch drops the effect, which must break ReadAfterWrite.
DoLostOverwrite(k, c) ==
    /\ opCount < MaxOps
    /\ IF LostResponseDropsEffect
           THEN UNCHANGED mcStoreVars             \* BROKEN: caller retries a lost effect
           ELSE PutOverwrite(k, c)                \* correct: effect applied, response lost
    /\ lastWritten' = [lastWritten EXCEPT ![k] = c]
    /\ UNCHANGED <<createWins, casStale, mpPublished>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

\* --- Multipart client actions -----------------------------------------------

DoMultipartBegin(u, k) ==
    /\ MultipartBegin(u, k)
    /\ mpPublished' = [mpPublished EXCEPT ![u] = FALSE]
    /\ UNCHANGED <<opCount, createWins, casStale, lastWritten, lastDeletedKey>>

DoMultipartPart(u, c) ==
    /\ MultipartPart(u, c)
    /\ UNCHANGED <<opCount, createWins, casStale, lastWritten, mpPublished, lastDeletedKey>>

DoMultipartComplete(u) ==
    /\ opCount < MaxOps
    /\ MultipartComplete(u)
    /\ lastWritten' = [lastWritten EXCEPT ![uploads[u].key] = uploads[u].content]
    /\ mpPublished' = [mpPublished EXCEPT ![u] = TRUE]
    /\ UNCHANGED <<createWins, casStale>>
    /\ lastDeletedKey' = NoKey
    /\ opCount' = opCount + 1

DoMultipartAbort(u) ==
    /\ MultipartAbort(u)
    /\ UNCHANGED <<opCount, createWins, casStale, lastWritten, mpPublished, lastDeletedKey>>

\* --- Listing client actions (not charged against the budget) -----------------

DoListBegin ==
    /\ ListBegin
    /\ UNCHANGED ghostVars

DoListReturn(k) ==
    /\ ListReturn(k)
    /\ UNCHANGED ghostVars

DoListProgress(k) ==
    /\ ListProgress(k)
    /\ UNCHANGED ghostVars

DoListEnd ==
    /\ ListEnd
    /\ UNCHANGED ghostVars

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

\* --- Invariants (the module's own semantics) --------------------------------

MCTypeOK ==
    /\ StoreTypeOK
    /\ opCount \in 0..MaxOps
    /\ createWins \in [Keys -> 0..MaxOps]
    /\ casStale \in BOOLEAN
    /\ lastWritten \in [Keys -> Content \cup {NoWrite}]
    /\ mpPublished \in [Clients -> BOOLEAN]
    /\ lastDeletedKey \in Keys \cup {NoKey}

\* At most one CreateIfAbsent wins per presence interval (a second create on a
\* present key gets AlreadyExists; a delete resets the interval).
CreateIfAbsentWinnerUnique == \A k \in Keys : createWins[k] <= 1

\* A successful CAS used the current version. (Broken by CasAcceptsStale.)
CasNeedsFreshVersion == ~casStale

\* Read-after-write: the latest durable write for a present key is visible.
\* (Broken by LostResponseDropsEffect, which claims a write that never landed.)
ReadAfterWrite ==
    \A k \in Keys :
        lastWritten[k] # NoWrite => (store[k].present /\ store[k].content = lastWritten[k])

\* Deletion is idempotent and total: after a delete the key is absent.
DeleteIdempotent ==
    lastDeletedKey # NoKey => ~store[lastDeletedKey].present

\* Nothing from an in-progress multipart upload is published before complete.
MultipartInvisibleUntilComplete ==
    \A u \in Clients : uploads[u].active => (mpPublished[u] = FALSE)

\* --- Liveness (checked against FairSpec only) -------------------------------

\* Every started listing eventually returns every key present when it started.
\* Fairness assumption: WF on MCListProgress (the list action) only.
ListEventuallyComplete ==
    listState.active ~> (listState.snapshot \subseteq listState.returned)

=============================================================================

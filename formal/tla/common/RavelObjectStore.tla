--------------------------- MODULE RavelObjectStore ---------------------------
(*****************************************************************************)
(* Shared object-store semantics for the Ravel TLA+ suite (ADR-1113 D2).    *)
(*                                                                           *)
(* This module encodes the contract in docs/object-store-contract.md and    *)
(* the behaviour of `MemoryStore` (the semantics oracle in                   *)
(* crates/ravel-object-store), and NOTHING about any specific protocol.      *)
(* Every protocol area (commit, catalog, lifecycle, resharding,             *)
(* maintenance) INSTANCEs or EXTENDs this module and drives its actions.    *)
(*                                                                           *)
(* --- Contract assumptions this module SUPPLIES (things Ravel relies on) -- *)
(*  * whole-object atomic visibility: a key is present with exactly one      *)
(*    content and one version, or absent (no partially-written object);      *)
(*  * CreateIfAbsent returns AlreadyExists on a present key                  *)
(*    (`PutMode::CreateIfAbsent`, `MemoryStore::put`);                       *)
(*  * CasVersion(v) returns PreconditionFailed on a version mismatch OR an   *)
(*    absent key, matching `MemoryStore::put`;                              *)
(*  * Overwrite always publishes (`PutMode::Overwrite`); multipart complete  *)
(*    publishes with Overwrite semantics;                                    *)
(*  * read-after-write and list-after-write for successful writes;           *)
(*  * a lost response: the effect is applied and the caller observes a       *)
(*    failure (models an ack loss; the durable effect is identical to a      *)
(*    success, only the caller's observation differs);                       *)
(*  * a transient failure: nothing is applied and the caller observes a      *)
(*    retryable error;                                                       *)
(*  * idempotent deletion (delete of an absent key is Ok);                   *)
(*  * paginated listing as a nondeterministic traversal: every key present   *)
(*    before the traversal started is eventually returned, a key created     *)
(*    during the traversal may or may not appear, and a key may appear more  *)
(*    than once (a consumer whose result depends on multiplicity MUST        *)
(*    deduplicate);                                                          *)
(*  * `last_modified` is a server-assigned advisory value. NO correctness    *)
(*    property may read it; it is exposed ONLY through                       *)
(*    ClaimExpiryReadLastModifiedAdvisory, the operator the claim-expiry     *)
(*    path (ADR-1029) is permitted to consult.                              *)
(*                                                                           *)
(* --- Properties Ravel must ESTABLISH over these assumptions --------------- *)
(*  Named safety invariants and liveness properties live in each protocol's  *)
(*  MC*.tla; MCRavelObjectStore.tla checks the module's own semantics.       *)
(*                                                                           *)
(* --- Behaviour OUT OF SCOPE (this module deliberately does not model) ----- *)
(*  * permanent loss of a durable object (a backend eating a committed       *)
(*    object): the durability argument is a backend assumption, not proved;  *)
(*  * a backend that violates its own contract (eventually-consistent        *)
(*    listing, a CAS that ignores its precondition): those are the failure   *)
(*    modes the runtime conformance suite (conformance.rs) probes, not this  *)
(*    model;                                                                  *)
(*  * commit-record reconstruction (ADR-0058), which derives a reconstructed *)
(*    created_unix_ns from last_modified: an assumption the commit area's     *)
(*    README records, not checked here.                                      *)
(*                                                                           *)
(* Payloads are abstract: Content is a small finite set standing in for a    *)
(* hash, versions are naturals. No SystemTime, no real hashes.               *)
(*                                                                           *)
(* Modelling note: in-progress multipart uploads are held in an ephemeral    *)
(* `uploads` variable (per caller). It is not durable store state; it exists *)
(* only so the "nothing visible until complete" clause can be modelled. The  *)
(* durable state is `store`; the advisory state is `lastModified`.           *)
(*****************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,       \* small finite set of object keys
    Content,    \* small finite set of contents; includes the NoContent sentinel
    NoContent,  \* distinguished "no content" element of Content
    Clients     \* callers; indexes the per-caller multipart handle

ASSUME NoContent \in Content
ASSUME Keys # {}

\* Real (writable) contents: everything but the sentinel.
RealContent == Content \ {NoContent}

VARIABLES
    store,         \* [Keys -> [present: BOOLEAN, content: Content, version: Nat]]
    lastModified,  \* [Keys -> Nat]  advisory; only claim-expiry may read it
    uploads,       \* [Clients -> [active: BOOLEAN, key: Keys, content: Content]]
    listState      \* [active: BOOLEAN, snapshot: SUBSET Keys, returned: SUBSET Keys]

storeVars == <<store, lastModified, uploads, listState>>

EmptyRec == [present |-> FALSE, content |-> NoContent, version |-> 0]

StoreTypeOK ==
    /\ store \in [Keys -> [present: BOOLEAN, content: Content, version: Nat]]
    /\ lastModified \in [Keys -> Nat]
    /\ uploads \in [Clients -> [active: BOOLEAN, key: Keys, content: Content]]
    /\ listState \in [active: BOOLEAN, snapshot: SUBSET Keys, returned: SUBSET Keys]

StoreInit ==
    /\ store = [k \in Keys |-> EmptyRec]
    /\ lastModified = [k \in Keys |-> 0]
    /\ uploads = [u \in Clients |->
                    [active |-> FALSE, key |-> CHOOSE k \in Keys : TRUE, content |-> NoContent]]
    /\ listState = [active |-> FALSE, snapshot |-> {}, returned |-> {}]

\* --- Accessors --------------------------------------------------------------

Present(k)   == store[k].present
ContentOf(k) == store[k].content
VersionOf(k) == store[k].version

\* The ONLY reader of last_modified. Named to make an illegitimate read on a
\* correctness path visible in a grep. Corresponds to the advisory age use in
\* ADR-1029 (claim expiry) and docs/object-store-contract.md "last_modified".
ClaimExpiryReadLastModifiedAdvisory(k) == lastModified[k]

\* --- Result functions (the caller-visible outcome, evaluated pre-state) -----

\* `MemoryStore::put` under CreateIfAbsent.
CreateResult(k) == IF store[k].present THEN "AlreadyExists" ELSE "Ok"

\* `MemoryStore::put` under CasVersion: PreconditionFailed on mismatch OR absent.
CasResult(k, v) ==
    IF (~store[k].present) \/ (store[k].version # v) THEN "PreconditionFailed" ELSE "Ok"

\* Read-after-write: a successful write is immediately visible.
GetResult(k) == IF store[k].present THEN store[k].content ELSE "NotFound"

\* --- Write effect -----------------------------------------------------------

\* A visible write: present, content c, a fresh (strictly larger) per-key
\* version. lastModified advances too; it is advisory, so a model that needs it
\* unreliable overrides this, but no correctness property reads it here.
WriteVisible(k, c) ==
    /\ store' = [store EXCEPT ![k] =
                    [present |-> TRUE, content |-> c, version |-> store[k].version + 1]]
    /\ lastModified' = [lastModified EXCEPT ![k] = store[k].version + 1]

\* --- Store actions (each names the Rust symbol / contract clause) -----------

\* PutMode::Overwrite -- always publishes.
PutOverwrite(k, c) ==
    /\ WriteVisible(k, c)
    /\ UNCHANGED <<uploads, listState>>

\* PutMode::CreateIfAbsent -- AlreadyExists (no effect) on a present key.
PutCreateIfAbsent(k, c) ==
    IF store[k].present
        THEN UNCHANGED storeVars
        ELSE PutOverwrite(k, c)

\* PutMode::CasVersion -- PreconditionFailed (no effect) on version mismatch or
\* absent key, matching `MemoryStore::put`.
PutCasVersion(k, v, c) ==
    IF (~store[k].present) \/ (store[k].version # v)
        THEN UNCHANGED storeVars
        ELSE PutOverwrite(k, c)

\* Idempotent delete (`ObjectStoreBackend::delete`, NotFound => Ok).
Delete(k) ==
    /\ store' = [store EXCEPT ![k] = EmptyRec]
    /\ lastModified' = [lastModified EXCEPT ![k] = store[k].version + 1]
    /\ UNCHANGED <<uploads, listState>>

\* --- Multipart (put_multipart / MultipartUpload): invisible until complete --

MultipartBegin(u, k) ==
    /\ ~uploads[u].active
    /\ uploads' = [uploads EXCEPT ![u] = [active |-> TRUE, key |-> k, content |-> NoContent]]
    /\ UNCHANGED <<store, lastModified, listState>>

MultipartPart(u, c) ==
    /\ uploads[u].active
    /\ uploads' = [uploads EXCEPT ![u].content = c]
    /\ UNCHANGED <<store, lastModified, listState>>

\* complete publishes exactly like Overwrite (no CreateIfAbsent/CasVersion path).
MultipartComplete(u) ==
    /\ uploads[u].active
    /\ uploads[u].content # NoContent
    /\ WriteVisible(uploads[u].key, uploads[u].content)
    /\ uploads' = [uploads EXCEPT ![u] =
                    [active |-> FALSE, key |-> uploads[u].key, content |-> NoContent]]
    /\ UNCHANGED listState

MultipartAbort(u) ==
    /\ uploads[u].active
    /\ uploads' = [uploads EXCEPT ![u] =
                    [active |-> FALSE, key |-> uploads[u].key, content |-> NoContent]]
    /\ UNCHANGED <<store, lastModified, listState>>

\* --- Listing: nondeterministic paginated traversal --------------------------

ListBegin ==
    /\ ~listState.active
    /\ listState' = [active |-> TRUE,
                     snapshot |-> {k \in Keys : store[k].present},
                     returned |-> {}]
    /\ UNCHANGED <<store, lastModified, uploads>>

\* May return a currently-present key (a key created during the traversal) OR a
\* snapshot key; adding an already-returned key models a duplicate delivery.
ListReturn(k) ==
    /\ listState.active
    /\ (store[k].present \/ k \in listState.snapshot)
    /\ listState' = [listState EXCEPT !.returned = @ \cup {k}]
    /\ UNCHANGED <<store, lastModified, uploads>>

\* The progress sub-action fairness is stated over: return an un-returned
\* snapshot key. A snapshot key stays returnable even if deleted mid-traversal.
ListProgress(k) ==
    /\ listState.active
    /\ k \in (listState.snapshot \ listState.returned)
    /\ listState' = [listState EXCEPT !.returned = @ \cup {k}]
    /\ UNCHANGED <<store, lastModified, uploads>>

ListEnd ==
    /\ listState.active
    /\ listState.snapshot \subseteq listState.returned
    /\ listState' = [active |-> FALSE, snapshot |-> {}, returned |-> {}]
    /\ UNCHANGED <<store, lastModified, uploads>>

=============================================================================

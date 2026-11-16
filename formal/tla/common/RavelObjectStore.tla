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
(*  * versions come from a single monotonic counter that a delete never      *)
(*    resets or reuses (`MemoryStore::next_id`), so create/delete/create     *)
(*    yields a fresh version and a CAS on a pre-delete token fails;          *)
(*  * read-after-write and list-after-write for successful writes;           *)
(*  * a lost response: the effect is applied and the caller observes a       *)
(*    Failure (models an ack loss; the durable effect is identical to a      *)
(*    success, only the caller's observation differs);                       *)
(*  * a transient failure: nothing is applied and the caller observes a       *)
(*    Failure (retryable);                                                    *)
(*  * idempotent deletion (delete of an absent key is Ok and changes no      *)
(*    observable state);                                                      *)
(*  * paginated listing as a nondeterministic traversal: every key present   *)
(*    before the traversal started is eventually returned, a key created     *)
(*    during the traversal may or may not appear, and a key may appear more  *)
(*    than once (deliveries are counted per key, so a duplicate is a         *)
(*    distinct step; a consumer whose result depends on multiplicity MUST    *)
(*    deduplicate);                                                          *)
(*  * `last_modified` is a server-assigned advisory value. NO correctness    *)
(*    property may read it; it is exposed ONLY through                       *)
(*    ClaimExpiryReadLastModifiedAdvisory, the operator the claim-expiry     *)
(*    path (ADR-1029) is permitted to consult.                              *)
(*                                                                           *)
(* --- Caller-visible outcomes ---------------------------------------------- *)
(*  Each write returns one of Ok, AlreadyExists, PreconditionFailed, or      *)
(*  Failure (a lost response or a transient failure). The result functions   *)
(*  below evaluate the outcome from the pre-state; the effect operators       *)
(*  apply the durable change the same outcome implies. A model relates the    *)
(*  two by recording the outcome next to the store delta, so an effect that   *)
(*  disagrees with its outcome (a CAS applied on a stale version, a lost      *)
(*  response that never landed) is a checkable violation, not a ghost.        *)
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
(* hash, versions are naturals drawn from the monotonic counter. No          *)
(* SystemTime, no real hashes.                                              *)
(*                                                                           *)
(* Modelling note: in-progress multipart uploads are held in an ephemeral    *)
(* `uploads` variable (per caller). It is not durable store state; it exists *)
(* only so the "nothing visible until complete" clause can be modelled. The  *)
(* durable state is `store` and its `versionCounter`; the advisory state is  *)
(* `lastModified`.                                                           *)
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

\* The contract only needs "a key may appear more than once" modelled; two
\* deliveries of one key exercise a duplicate without an unbounded traversal.
MaxListMultiplicity == 2

VARIABLES
    store,          \* [Keys -> [present: BOOLEAN, content: Content, version: Nat]]
    lastModified,   \* [Keys -> Nat]  advisory; only claim-expiry may read it
    versionCounter, \* Nat  monotonic version source; a delete never resets it
    uploads,        \* [Clients -> [active: BOOLEAN, key: Keys, content: Content]]
    listState       \* [active: BOOLEAN, snapshot: SUBSET Keys,
                    \*  delivered: [Keys -> 0..MaxListMultiplicity]]

storeVars == <<store, lastModified, versionCounter, uploads, listState>>

EmptyRec == [present |-> FALSE, content |-> NoContent, version |-> 0]

StoreTypeOK ==
    /\ store \in [Keys -> [present: BOOLEAN, content: Content, version: Nat]]
    /\ lastModified \in [Keys -> Nat]
    /\ versionCounter \in Nat
    /\ uploads \in [Clients -> [active: BOOLEAN, key: Keys, content: Content]]
    /\ listState \in [active: BOOLEAN, snapshot: SUBSET Keys,
                      delivered: [Keys -> 0..MaxListMultiplicity]]

StoreInit ==
    /\ store = [k \in Keys |-> EmptyRec]
    /\ lastModified = [k \in Keys |-> 0]
    /\ versionCounter = 0
    /\ uploads = [u \in Clients |->
                    [active |-> FALSE, key |-> CHOOSE k \in Keys : TRUE, content |-> NoContent]]
    /\ listState = [active |-> FALSE, snapshot |-> {}, delivered |-> [k \in Keys |-> 0]]

\* --- Accessors --------------------------------------------------------------

Present(k)   == store[k].present
ContentOf(k) == store[k].content
VersionOf(k) == store[k].version

\* Keys the traversal has delivered at least once (the deduplicated view).
Delivered == {k \in Keys : listState.delivered[k] > 0}

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

\* Overwrite and idempotent delete always succeed.
OverwriteResult == "Ok"
DeleteResult    == "Ok"

\* Read-after-write: a successful write is immediately visible.
GetResult(k) == IF store[k].present THEN store[k].content ELSE "NotFound"

\* --- Write effect -----------------------------------------------------------

\* A visible write: present, content c, a fresh version minted from the global
\* monotonic counter. lastModified advances too; it is advisory, so a model that
\* needs it unreliable overrides this, but no correctness property reads it here.
WriteVisible(k, c) ==
    /\ store' = [store EXCEPT ![k] =
                    [present |-> TRUE, content |-> c, version |-> versionCounter + 1]]
    /\ versionCounter' = versionCounter + 1
    /\ lastModified' = [lastModified EXCEPT ![k] = versionCounter + 1]

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

\* Idempotent delete (`ObjectStoreBackend::delete`, NotFound => Ok). Deleting an
\* absent key is a total no-op: store, counter, and advisory time all unchanged.
\* The version counter is NOT reset, so a later create draws a fresh version.
Delete(k) ==
    /\ store' = [store EXCEPT ![k] = EmptyRec]
    /\ UNCHANGED <<lastModified, versionCounter, uploads, listState>>

\* --- Lost responses and transient failures ----------------------------------
\* A lost-response write applies the SAME durable effect as its success case;
\* the caller observes Failure and may retry. A transient failure applies
\* nothing and the caller observes Failure. Callers see the outcome; the durable
\* store cannot tell a success from a lost response.

PutOverwriteLostResponse(k, c)     == PutOverwrite(k, c)
PutCreateIfAbsentLostResponse(k, c) == PutCreateIfAbsent(k, c)
PutCasVersionLostResponse(k, v, c) == PutCasVersion(k, v, c)
DeleteLostResponse(k)              == Delete(k)

TransientFailure == UNCHANGED storeVars

\* --- Multipart (put_multipart / MultipartUpload): invisible until complete --

MultipartBegin(u, k) ==
    /\ ~uploads[u].active
    /\ uploads' = [uploads EXCEPT ![u] = [active |-> TRUE, key |-> k, content |-> NoContent]]
    /\ UNCHANGED <<store, lastModified, versionCounter, listState>>

MultipartPart(u, c) ==
    /\ uploads[u].active
    /\ uploads' = [uploads EXCEPT ![u].content = c]
    /\ UNCHANGED <<store, lastModified, versionCounter, listState>>

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
    /\ UNCHANGED <<store, lastModified, versionCounter, listState>>

\* --- Listing: nondeterministic paginated traversal --------------------------
\* `delivered` is a per-key delivery count (a bag), so a duplicate delivery is a
\* distinct step. Completeness is stated over the deduplicated support `Delivered`.

ListBegin ==
    /\ ~listState.active
    /\ listState' = [active |-> TRUE,
                     snapshot |-> {k \in Keys : store[k].present},
                     delivered |-> [k \in Keys |-> 0]]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads>>

\* May deliver a currently-present key (created during the traversal) OR a
\* snapshot key; re-delivering an already-delivered key (count -> 2) models a
\* duplicate page entry, bounded by MaxListMultiplicity.
ListReturn(k) ==
    /\ listState.active
    /\ (store[k].present \/ k \in listState.snapshot)
    /\ listState.delivered[k] < MaxListMultiplicity
    /\ listState' = [listState EXCEPT !.delivered[k] = @ + 1]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads>>

\* The progress sub-action fairness is stated over: deliver an as-yet-undelivered
\* snapshot key. A snapshot key stays deliverable even if deleted mid-traversal.
ListProgress(k) ==
    /\ listState.active
    /\ k \in listState.snapshot
    /\ listState.delivered[k] = 0
    /\ listState' = [listState EXCEPT !.delivered[k] = 1]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads>>

ListEnd ==
    /\ listState.active
    /\ listState.snapshot \subseteq Delivered
    /\ listState' = [active |-> FALSE, snapshot |-> {}, delivered |-> [k \in Keys |-> 0]]
    /\ UNCHANGED <<store, lastModified, versionCounter, uploads>>

=============================================================================

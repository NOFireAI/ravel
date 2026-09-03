--------------------------- MODULE MCCatalogMVCC ---------------------------
(*****************************************************************************)
(* Model-check entry for CatalogMVCC (ADR-1113 D5, task T3). It fixes the    *)
(* small constant sets in the .cfg files, defines the folder-symmetry set,   *)
(* and owns the negative-control switch defaults (every non-negative .cfg    *)
(* sets all six switches FALSE; each negative/<name>.cfg flips exactly one).  *)
(*                                                                           *)
(* Folders are the object-store Clients, so client symmetry is folder        *)
(* symmetry. It is used under the safety configs only; the exhaustive        *)
(* liveness config drops it, matching the common area's convention.          *)
(*****************************************************************************)
EXTENDS CatalogMVCC, TLC

Symmetry == Permutations(Clients)

=============================================================================

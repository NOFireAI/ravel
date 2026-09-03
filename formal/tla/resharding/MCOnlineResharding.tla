--------------------------- MODULE MCOnlineResharding ---------------------------
(***************************************************************************)
(* Model-check entry for OnlineResharding: the finite configuration lives  *)
(* in the .cfg files beside this module, and the two negative-control      *)
(* switches (WriterFenceEnabled, TokenValidatedAgainstCount) default to    *)
(* their correct values there. The constants a negative control flips      *)
(* instead of a switch (S, L, AppenderSkew) are ordinary model bounds, so  *)
(* a negative config assigns a different number rather than turning        *)
(* behavior off.                                                          *)
(***************************************************************************)
EXTENDS OnlineResharding

(***************************************************************************)
(* Writers are interchangeable: a writer's identity appears only as the    *)
(* owner of a cached view, an open flush, and its own commit keys, and     *)
(* every invariant and the liveness property quantify over all writers, so *)
(* permuting two writers maps a behavior to a behavior. Requesters are     *)
(* interchangeable for the same reason: a request carries no identity      *)
(* beyond its own progress, and each one picks its target count from the   *)
(* same set, so the two orders (increase first, decrease first) are both   *)
(* reachable under either naming.                                          *)
(*                                                                        *)
(* Used by the smoke config only. TLC's symmetry reduction is not sound in *)
(* general for a temporal property, and the exhaustive config is the one   *)
(* that checks EventuallyRoutedOnNewGeneration.                            *)
(***************************************************************************)
Symmetry == Permutations(Writers) \cup Permutations(Requesters)

MCSpec == Spec
MCFairSpec == FairSpec

=============================================================================

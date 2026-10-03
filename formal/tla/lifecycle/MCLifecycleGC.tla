---------------------------- MODULE MCLifecycleGC ----------------------------
(*****************************************************************************)
(* Model-check entry module for the lifecycle area (ADR-1113 D5, task T4).    *)
(*                                                                           *)
(* The whole model, its actions, invariants, fairness and the negative-control *)
(* CONSTANT switches live in LifecycleGC.tla. This module is the entry the      *)
(* harness runs: the cfg pins the finite instance's horizon constants and the   *)
(* switch values (all FALSE / the correct value in smoke.cfg and                *)
(* exhaustive.cfg; each negative cfg flips exactly one). Keeping the switches   *)
(* in the shared spec means a negative control is one constant value away from  *)
(* the checked model, never a second copy of the spec.                          *)
(*****************************************************************************)
EXTENDS LifecycleGC

===============================================================================

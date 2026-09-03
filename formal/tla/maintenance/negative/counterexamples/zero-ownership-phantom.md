# zero-ownership-phantom

Switch: `Phantom = TRUE`. Expected: `EveryEligibleUnitEventuallyAttempted` violated (exit 13, temporal).

Trace: with the phantom injected, every worker's live-set computation includes
the phantom member `PH`, whose rendezvous weight (`PhantomWeight`) outranks every
real worker. In the behaviour TLC finds, each worker recomputes its live set
(`ComputeLive`) before it attempts unit 1; from then on `Owner(1, cachedLive[w])`
is `PH` for every worker `w`, so no `WorkerRecord` step is ever enabled and
`attemptedByOwner[1]` stays false forever. The eventuality `<>attemptedByOwner[1]`
fails; the run stutters with the unit unattended.

Classification: a documented liveness limitation, not a defect. `PH` stands for
the lingering heartbeat key of a departed or restarted worker (a fresh process id
leaves the old key in place and it is never deleted). If that key is within the
liveness window and outranks the live workers, discovery leaves the unit
unattended -- ADR-0065 accepts this asymmetric-view case. Correctness never
depends on it: the ungated CLI path still publishes the unit correctly, and no
safety invariant is affected.

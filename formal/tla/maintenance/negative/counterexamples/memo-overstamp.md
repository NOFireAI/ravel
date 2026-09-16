# memo-overstamp

Switch: `MemoOverstamp = TRUE`. Expected: `MemoNeverExtendsFreshnessPastSnapshot` violated (exit 12).

Trace: worker 1 writes a future entry via `FutureEntry` -- a snapshot whose entry
`verU` is one tick ahead of the snapshot's own `snapNs` (a clock ahead of the
snapshot-writing clock). A seeder then takes the broken `BrokenSeed` step, which
reads the raw `verU` without clamping it to the source snapshot's `snapNs`. The
witness records `val > bound`, so `lastMaint.class = "seed" => lastMaint.val =< lastMaint.bound`
fails.

Why it matters: without the `verified_ns = min(verified_ns, snapshot_unix_ns)`
clamp, a skewed entry reads as eternally fresh and suppresses re-verification of
a bucket forever. The shipped `seed_from_snapshot` clamps every entry.

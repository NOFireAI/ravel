# Negative control: complete-ignores-served-set

Switch: `CompleteIgnoresServedSet = TRUE` (completion skips the served-set check,
writing the `.done` marker while the current HEAD still serves the erased
subject). All other switches at base.

Target invariant: `CompletionImpliesNoPreRewriteExposure`. TLC exit 12.

```
Error: Invariant CompletionImpliesNoPreRewriteExposure is violated.
```

Trace: `RequestErasure` records the erasure of subject `s1`. `CompleteErasure`
then writes the `.done` marker `doneR1` (present at the final state) while
`head = {raw1}`, `headState = present`, and `objContent["raw1"]` still serves
`s1`. With the switch on, the completion action drops the
`~ServesNow("s1")` guard, so the marker is written despite the live exposure. The
invariant reads the present `.done` marker and the current HEAD content and
requires that no head-named present object still serves the erased subject;
because `raw1` does, the invariant fails at the `CompleteErasure` state.

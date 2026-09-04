# Negative control: rewrite-keeps-erased-records

Switch: `RewriteKeepsErasedRecords = TRUE` (the rewrite output keeps the records
whose subject the applied requests erased, instead of dropping them). All other
switches at base.

Target invariant: `RewriteOutputsAreInputsMinusErased`. TLC exit 12.

```text
Error: Invariant RewriteOutputsAreInputsMinusErased is violated.
```

Trace: `PerformRewrite` materialises `rwA` from the raw inputs. With the switch
on, `RewriteOutputContent` retains a record whose subject the applied erasure
request removed, so the final `objContent["rwA"] = {rec1}` and `store["rwA"]` is
present. The invariant reads the materialised output content and requires it to
exclude every erased subject; because `rwA` still serves that subject the
invariant fails at the `PerformRewrite` state.

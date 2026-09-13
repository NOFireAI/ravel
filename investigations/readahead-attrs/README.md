# Read-ahead and attribute-extraction investigation

Refs: #913 (epic: cache-independent query performance).

## Revision

All code references in this directory are pinned to
`ee2070a00f41b1add0d3225d420fbc7313112e06`, which was `origin/main` at
dispatch time (2026-09-13). Every run in `LEDGER.md` records this revision
plus the exact diff (if any) that was applied on top of it.

## Box

Fleet executor, amd64 class:

```
uname -m : x86_64
uname -a : Linux ip-172-31-18-6 7.0.0-1011-aws #11~24.04.1-Ubuntu SMP PREEMPT Mon Aug 10 15:20:57 UTC 2026 x86_64
nproc    : 16
free -g  : Mem total 30, free 26, available 28; Swap 0
df -h /tmp :  /dev/root  484G  129G  356G  27%  /
df -h .    :  /dev/root  484G  129G  356G  27%  /var/lib/fleet
df -h $HOME:  tmpfs      1.0G   16K  1.0G   1%  /var/lib/fleet/secrets   (1 GB tmpfs)
toolchain: cargo 1.97.1, rustc 1.97.1; cargo-nextest present
```

`CARGO_BUILD_JOBS=4` is set on every cargo invocation, as the dispatch
requires. Build logs go to `.gate-logs/` (gitignored). Deliverables live in
this directory only.

## Blockers (named, not substituted)

See `BLOCKERS.md` for the full list. In short:

- No real S3, no AWS credential, no authorized ClickBench reference host.
  Every wall-clock figure in this directory is labelled with the backend
  that produced it (in-process `MemoryStore`, or a wrapped reference store).
  None is an S3 number and none is extrapolated to S3.
- No docker, no podman, no MinIO binary on the box (`which` returns
  nothing for all three). The MinIO lane is not run.
- No GitHub access (no gh token, proxy blocks the API). Issue and PR state
  is taken as pasted into the dispatch spec.

## Files

| File | Content |
|---|---|
| `README.md` | this file: revision, box, blockers summary |
| `S1-VERIFICATION.md` | code-observed check of the carried findings |
| `CLASSIFICATION.md` | one table, one class per row |
| `LEDGER.md` | every run: revision, diff, commands, config, raw results, pre-registered bands |
| `RECOMMENDATION.md` | the measured recommendation with limits |
| `WORKITEMS.md` | ranked, bounded work items (draft only) |
| `BLOCKERS.md` | exact blockers for anything unmeasured |
| `diagrams/` | execution and buffer-lifetime diagrams |
| `results/` | raw result JSON from bench runs |

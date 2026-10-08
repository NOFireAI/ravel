#!/usr/bin/env python3
"""Attribute a `jeprof --collapsed --show_bytes` profile to allocation sites.

usage: attr.py <collapsed.txt> <title> [top-n]

Three groupings of the same bytes, each summing to the profile total:
  site   - the first frame below the allocator that is not std/core/alloc
           plumbing (Vec/RawVec/Box/hashbrown growth), i.e. the code that
           asked for the memory;
  crate  - that site's crate;
  ravel  - the innermost (closest to the allocation) frame in a ravel_*
           crate on the stack, or "<no ravel frame>".
"""
import re
import sys
from collections import Counter

ALLOC_MARK = re.compile(r"__rustc::__rust_(alloc|realloc|alloc_zeroed)|_rjem_je_|prof_backtrace")
PLUMBING = re.compile(
    r"^(<)*(alloc::|core::|std::|hashbrown::|__rustc::|tikv_jemalloc|_rjem|imalloc|prof_|"
    r"<alloc::|<core::|<std::|<hashbrown::)"
)
CRATE = re.compile(r"^[<&\s]*(?:dyn\s+)?([a-z_][a-z0-9_]*)::")
SUFFIX = re.compile(r"(<[0-9a-f]{16}>|\[inline\])$")


def clean(frame):
    return SUFFIX.sub("", frame).strip()


def crate_of(frame):
    m = CRATE.match(frame)
    return m.group(1) if m else "?"


def main():
    path, title = sys.argv[1], sys.argv[2]
    top = int(sys.argv[3]) if len(sys.argv) > 3 else 40
    site, crate, ravel = Counter(), Counter(), Counter()
    total = 0
    for line in open(path):
        line = line.rstrip("\n")
        if not line:
            continue
        stack, _, count = line.rpartition(" ")
        n = int(count)
        total += n
        frames = [clean(f) for f in stack.split(";")]
        # Leaf is last. Drop everything from the allocator entry point down.
        cut = len(frames)
        for i, f in enumerate(frames):
            if ALLOC_MARK.search(f):
                cut = i
                break
        caller = frames[:cut]
        s = next((f for f in reversed(caller) if not PLUMBING.match(f)), caller[-1] if caller else "?")
        site[s] += n
        crate[crate_of(s)] += n
        r = next((f for f in reversed(caller) if re.search(r"(^|[<\s&])ravel_[a-z_]+::", f)), "<no ravel frame>")
        ravel[r] += n
    print(f"# {title}")
    print(f"# total {total} B ({total / 1e9:.3f} GB)")
    for name, ctr in (("site (first non-plumbing frame below the allocator)", site),
                      ("crate of that site", crate),
                      ("innermost ravel_* frame on the stack", ravel)):
        print(f"\n## top {top} by {name}")
        print(f"{'bytes':>14} {'share':>6} {'cum':>6}  frame")
        acc = 0
        for k, v in ctr.most_common(top):
            acc += v
            print(f"{v:>14} {100 * v / total:5.1f}% {100 * acc / total:5.1f}%  {k[:220]}")


if __name__ == "__main__":
    main()

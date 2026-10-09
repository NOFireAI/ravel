#!/usr/bin/env python3
"""Caller chains behind one attr.py site in a `jeprof --collapsed` profile.

usage: stacks.py <collapsed.txt> <site-substring> [depth] [top-n]
Groups the stacks whose first non-plumbing frame (attr.py's rule) contains
<site-substring> by the next <depth> non-plumbing caller frames, and prints
bytes per chain.
"""
import sys
from collections import Counter

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from attr import ALLOC_MARK, PLUMBING, clean  # noqa: E402


def main():
    path, needle = sys.argv[1], sys.argv[2]
    depth = int(sys.argv[3]) if len(sys.argv) > 3 else 6
    top = int(sys.argv[4]) if len(sys.argv) > 4 else 8
    chains = Counter()
    total = 0
    for line in open(path):
        stack, _, n = line.rstrip("\n").rpartition(" ")
        if not n.lstrip("-").isdigit():
            continue
        frames = [clean(f) for f in stack.split(";")]
        # Same cut as attr.py: root-first, drop from the first allocator frame.
        cut = next((i for i, f in enumerate(frames) if ALLOC_MARK.search(f)), len(frames))
        rest = [f for f in reversed(frames[:cut]) if not PLUMBING.match(f)]
        if not rest or needle not in rest[0]:
            continue
        total += int(n)
        chains[" <- ".join(f[:110] for f in rest[1 : 1 + depth])] += int(n)
    print(f"# {needle}: {total} B")
    for chain, b in chains.most_common(top):
        print(f"{b:>12}  {chain}")


if __name__ == "__main__":
    main()

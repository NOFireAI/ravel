#!/usr/bin/env python3
"""Print the `run:` script of one named step in a workflow file.

Used by scripts/coderabbit-acceptance-tests.sh so the acceptance suite executes
the shell that actually ships, rather than a copy of it that can drift. Written
without a YAML dependency on purpose: the acceptance suite has to run on a bare
checkout, and Ravel's toolchain does not carry PyYAML.
"""

from __future__ import annotations

import sys


def main() -> int:
    if len(sys.argv) != 3:
        print("usage: extract-run-block.py <workflow-file> <step-name>", file=sys.stderr)
        return 2
    path, step_name = sys.argv[1], sys.argv[2]
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().split("\n")

    needle = f"- name: {step_name}"
    start = None
    for index, line in enumerate(lines):
        if line.strip() == needle:
            start = index
            break
    if start is None:
        print(f"step not found: {step_name}", file=sys.stderr)
        return 1

    run_at = None
    for index in range(start, len(lines)):
        if lines[index].strip() == "run: |":
            run_at = index
            break
        if lines[index].strip().startswith("- name:") and index != start:
            break
    if run_at is None:
        print(f"step has no `run: |` block: {step_name}", file=sys.stderr)
        return 1

    indent = len(lines[run_at]) - len(lines[run_at].lstrip())
    body_indent = indent + 2
    out: list[str] = []
    for line in lines[run_at + 1 :]:
        if not line.strip():
            out.append("")
            continue
        if len(line) - len(line.lstrip()) < body_indent:
            break
        out.append(line[body_indent:])
    print("\n".join(out).rstrip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

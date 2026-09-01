#!/usr/bin/env python3
"""Decide whether one exact (head, policy, CLI version) was already reviewed.

Reads a JSON array of pull-request review bodies on stdin, prints `1` if one of
them carries this integration's deduplication marker for the given triple, and
`0` otherwise.

The match is structural and per body. It requires a real HTML comment, and the
three fields must appear inside the same comment. Two properties make that
trustworthy:

  * the caller passes only bodies authored by `github-actions[bot]`, an
    identity no contributor can post as;
  * `render-findings.py` strips `<!--` and `-->` from every untrusted string it
    emits and breaks the marker's own name with a character entity, so a
    CodeRabbit finding whose text a contributor influenced cannot plant a marker
    for a head SHA that was never reviewed and suppress its review.

Concatenating the bodies before matching would defeat both: three fields spread
over three unrelated reviews would read as one marker.
"""

from __future__ import annotations

import json
import re
import sys

MARKER_RE = re.compile(
    r"<!--\s*coderabbit-maintainer-review\s+"
    r"head=(?P<head>[0-9a-f]{40})\s+"
    r"config=(?P<config>[0-9a-f]{64})\s+"
    r"version=(?P<version>[0-9A-Za-z._-]{1,32})\s*-->"
)

MAX_INPUT_BYTES = 4 * 1024 * 1024


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: find-marker.py <head-sha> <config-hash> <cli-version>", file=sys.stderr)
        return 2
    head, config_hash, version = sys.argv[1], sys.argv[2], sys.argv[3]

    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        print("review bodies exceeded the input bound; failing closed", file=sys.stderr)
        return 1
    try:
        bodies = json.loads(raw.decode("utf-8", errors="replace") or "[]")
    except ValueError:
        print("review bodies were not valid JSON; failing closed", file=sys.stderr)
        return 1
    if not isinstance(bodies, list):
        print("expected a JSON array of review bodies; failing closed", file=sys.stderr)
        return 1

    for body in bodies:
        if not isinstance(body, str):
            continue
        for match in MARKER_RE.finditer(body):
            if (
                match.group("head") == head
                and match.group("config") == config_hash
                and match.group("version") == version
            ):
                print("1")
                return 0
    print("0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

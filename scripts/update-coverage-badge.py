#!/usr/bin/env python3
"""Push a shields.io endpoint-badge JSON to the coverage gist.

Reads coverage.json (cargo-llvm-cov --json --summary-only output) and
PATCHes https://gist.github.com/pmoust/b45c736cf13204279b05507186c24325
with a shields.io schema payload. Requires GIST_PAT in the environment
(a classic PAT scoped to gist only); CI skips this script entirely when
the secret is absent.
"""

import json
import os
import sys
import urllib.request

GIST_ID = "b45c736cf13204279b05507186c24325"


def badge_color(pct: float) -> str:
    if pct >= 80:
        return "brightgreen"
    if pct >= 60:
        return "green"
    if pct >= 40:
        return "yellow"
    if pct >= 20:
        return "orange"
    return "red"


def main() -> int:
    token = os.environ.get("GIST_PAT")
    if not token:
        print("GIST_PAT not set, skipping badge update", file=sys.stderr)
        return 0

    with open("coverage.json") as f:
        totals = json.load(f)["data"][0]["totals"]["lines"]
    pct = totals["percent"]

    badge = {
        "schemaVersion": 1,
        "label": "coverage",
        "message": f"{pct:.1f}%",
        "color": badge_color(pct),
    }
    body = json.dumps(
        {"files": {"coverage.json": {"content": json.dumps(badge)}}}
    ).encode()

    req = urllib.request.Request(
        f"https://api.github.com/gists/{GIST_ID}",
        data=body,
        method="PATCH",
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(req) as resp:
        print(f"gist updated: HTTP {resp.status}, coverage {pct:.1f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

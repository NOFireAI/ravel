#!/usr/bin/env python3
"""Turn CodeRabbit CLI agent output into one GitHub pull-request review body.

Loaded from the trusted `main` checkout by
.github/workflows/coderabbit-maintainer-review.yml and run with a working
directory outside the reviewed worktree. The CLI's output is a model's text and
is treated as hostile input throughout: it is parsed with a strict JSON parser,
bounded in every dimension, stripped of control characters, and neutralised for
GitHub mentions and raw HTML before it reaches a review body. Nothing it
contains selects a repository, a pull request, an API endpoint, a review state,
a workflow command, or a shell command; those all arrive as argv from the
workflow, which took them from the GitHub API.

Agent output carries no line numbers (CodeRabbit CLI 0.7.3 emits
{"type":"finding","severity","fileName","comment"|"codegenInstructions",
"suggestions"}), so the review is grouped by file rather than anchored to lines.
That is the designed output shape, not a degraded fallback.

Writes a JSON request body for:
    gh api repos/{owner}/{repo}/pulls/{n}/reviews --input <out>
"""

from __future__ import annotations

import argparse
import json
import re
import sys

# Bounds. Every one of these is a cost or a blast-radius control, not a
# formatting preference.
MAX_INPUT_BYTES = 2 * 1024 * 1024
MAX_LINES = 20000
MAX_FINDINGS = 20
MAX_COMMENT_CHARS = 1200
MAX_SUGGESTIONS_PER_FINDING = 3
MAX_SUGGESTION_CHARS = 1500
# GitHub's hard limit on a review body is 65536 characters. Stay clear of it.
MAX_BODY_CHARS = 60000

SEVERITY_ORDER = ("critical", "major", "minor", "info")
# `info` is dropped: it is where style, praise, and low-confidence observations
# land, and Ravel's review policy asks for neither.
REPORTED_SEVERITIES = ("critical", "major", "minor")

MARKER_NAME = "coderabbit-maintainer-review"

SHA_RE = re.compile(r"\A[0-9a-f]{40}\Z")
HEX_RE = re.compile(r"\A[0-9a-f]{64}\Z")
VERSION_RE = re.compile(r"\A[0-9A-Za-z._-]{1,32}\Z")

# C0 and C1 controls except newline and tab, plus the bidirectional overrides
# and the byte-order mark. Terminal escape sequences and text that renders
# differently from how it parses both start here.
CONTROL_RE = re.compile(
    "[\\x00-\\x08\\x0b-\\x1f\\x7f-\\x9f"
    "\\u200b-\\u200f\\u202a-\\u202e\\u2066-\\u2069\\ufeff]"
)
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def sanitize(text: str, *, fenced: bool) -> str:
    """Neutralise one untrusted string.

    `fenced` text ends up inside a fenced code block, where GitHub neither
    linkifies mentions nor interprets HTML, so it only needs control-character
    and fence-escape handling. Unfenced prose additionally gets its `@` and `<`
    neutralised, and any HTML comment delimiters removed so the output cannot
    forge this integration's own deduplication marker.
    """
    if not isinstance(text, str):
        return ""
    text = ANSI_RE.sub("", text)
    text = CONTROL_RE.sub("", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    if fenced:
        # Break any attempt to close our fence early.
        return text.replace("```", "'''")
    text = text.replace("<!--", "").replace("-->", "")
    # Break the deduplication marker's own name. It renders identically and no
    # longer matches a byte-exact search, so a finding cannot plant a marker
    # that suppresses the review of a head SHA that has not been reviewed.
    text = text.replace(MARKER_NAME, MARKER_NAME.replace("-", "&#45;"))
    # `&#64;` renders as `@` and does not notify anyone.
    text = text.replace("@", "&#64;")
    # Raw HTML in prose becomes visible text rather than markup.
    text = text.replace("<", "&lt;").replace(">", "&gt;")
    # A line that starts with `::` is a workflow command if it ever reaches a
    # runner's stdout. Nothing here does, but neutralising it costs nothing and
    # removes the question.
    text = re.sub(r"^::", "&#58;&#58;", text, flags=re.MULTILINE)
    return text


def clip(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + " [truncated]"


def parse_findings(path: str) -> tuple[list[dict], list[str]]:
    """Parse agent JSONL. Returns (findings, notes-about-what-was-dropped)."""
    notes: list[str] = []
    with open(path, "rb") as handle:
        raw = handle.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise SystemExit(
            f"coderabbit output exceeded {MAX_INPUT_BYTES} bytes; refusing to parse"
        )
    text = raw.decode("utf-8", errors="replace")

    lines = text.split("\n")
    if len(lines) > MAX_LINES:
        raise SystemExit(f"coderabbit output exceeded {MAX_LINES} lines; refusing to parse")

    findings: list[dict] = []
    malformed = 0
    for line in lines:
        line = line.strip()
        if not line or not line.startswith("{"):
            # The CLI interleaves human-readable progress with the JSON lines.
            continue
        try:
            obj = json.loads(line)
        except (ValueError, RecursionError):
            malformed += 1
            continue
        if not isinstance(obj, dict) or obj.get("type") != "finding":
            malformed += 1
            continue

        severity = obj.get("severity")
        if severity not in SEVERITY_ORDER:
            severity = "info"
        file_name = obj.get("fileName")
        file_name = file_name if isinstance(file_name, str) else ""

        comment = obj.get("comment")
        if not isinstance(comment, str) or not comment.strip():
            comment = obj.get("codegenInstructions")
        if not isinstance(comment, str):
            comment = ""

        suggestions = obj.get("suggestions")
        if not isinstance(suggestions, list):
            suggestions = []
        suggestions = [s for s in suggestions if isinstance(s, str)]

        if not comment.strip() and not suggestions:
            continue

        findings.append(
            {
                "severity": severity,
                "file": file_name,
                "comment": comment,
                "suggestions": suggestions,
            }
        )

    if malformed:
        notes.append(f"{malformed} output line(s) were malformed or unrecognised and dropped.")
    return findings, notes


def select(findings: list[dict], notes: list[str]) -> list[dict]:
    dropped_info = sum(1 for f in findings if f["severity"] not in REPORTED_SEVERITIES)
    if dropped_info:
        notes.append(f"{dropped_info} informational finding(s) dropped by policy.")

    seen: set[tuple[str, str, str]] = set()
    unique: list[dict] = []
    duplicates = 0
    for finding in findings:
        if finding["severity"] not in REPORTED_SEVERITIES:
            continue
        key = (
            finding["severity"],
            finding["file"],
            re.sub(r"\s+", " ", finding["comment"]).strip()[:200].lower(),
        )
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        unique.append(finding)
    if duplicates:
        notes.append(f"{duplicates} duplicate finding(s) collapsed.")

    unique.sort(key=lambda f: (SEVERITY_ORDER.index(f["severity"]), f["file"]))
    if len(unique) > MAX_FINDINGS:
        notes.append(
            f"{len(unique) - MAX_FINDINGS} further finding(s) withheld; "
            f"this review reports at most {MAX_FINDINGS}."
        )
        unique = unique[:MAX_FINDINGS]
    return unique


def render(args: argparse.Namespace, findings: list[dict], notes: list[str]) -> str:
    marker = (
        f"<!-- {MARKER_NAME}\n"
        f"     head={args.head_sha}\n"
        f"     config={args.config_hash}\n"
        f"     version={args.cli_version}\n"
        "-->"
    )

    counts = {s: sum(1 for f in findings if f["severity"] == s) for s in REPORTED_SEVERITIES}
    parts: list[str] = [marker, "", "## CodeRabbit review (maintainer-dispatched)", ""]
    parts.append(
        "| | |\n|---|---|\n"
        f"| Reviewed head | `{args.head_sha}` |\n"
        f"| Diff base | `{args.base_sha}` |\n"
        f"| Trusted policy hash | `{args.config_hash}` |\n"
        f"| CodeRabbit CLI | `{args.cli_version}` |\n"
        f"| Dispatched by | `{args.actor}` (`{args.actor_role}`) |\n"
    )
    parts.append("")
    if args.reason:
        parts.append(f"Repeat review requested. Reason: {sanitize(args.reason, fenced=False)}")
        parts.append("")

    if findings:
        parts.append(
            f"**{counts['critical']} critical, {counts['major']} major, "
            f"{counts['minor']} minor.** Findings are grouped by file. The CLI's "
            "agent output carries no line numbers, so nothing here is anchored to "
            "a line; open the named file at the reviewed head."
        )
    else:
        parts.append(
            "**No critical, major, or minor findings.** This is a machine review "
            "of one revision. It is not an approval and it does not replace human "
            "review or `scripts/gates.sh`."
        )
    parts.append("")

    by_file: dict[str, list[dict]] = {}
    for finding in findings:
        by_file.setdefault(finding["file"] or "(file not reported)", []).append(finding)

    for file_name, group in by_file.items():
        parts.append(f"### `{sanitize(file_name, fenced=False)}`")
        parts.append("")
        for finding in group:
            body = clip(sanitize(finding["comment"], fenced=False).strip(), MAX_COMMENT_CHARS)
            parts.append(f"**{finding['severity']}** — {body}" if body else f"**{finding['severity']}**")
            for suggestion in finding["suggestions"][:MAX_SUGGESTIONS_PER_FINDING]:
                snippet = clip(sanitize(suggestion, fenced=True).strip(), MAX_SUGGESTION_CHARS)
                if snippet:
                    parts.append("")
                    parts.append("```")
                    parts.append(snippet)
                    parts.append("```")
            parts.append("")

    if notes:
        parts.append("<details><summary>Filtering notes</summary>")
        parts.append("")
        for note in notes:
            parts.append(f"- {sanitize(note, fenced=False)}")
        parts.append("")
        parts.append("</details>")
        parts.append("")

    parts.append(
        "---\nProduced by `.github/workflows/coderabbit-maintainer-review.yml` "
        "under the policy in `.github/coderabbit/` on `main` (ADR-0091). "
        "Read-only: this integration cannot push commits, open issues, or reply."
    )

    body = "\n".join(parts)
    if len(body) > MAX_BODY_CHARS:
        keep = MAX_BODY_CHARS - 200
        body = body[:keep].rstrip() + (
            "\n\n---\n*Review body truncated at "
            f"{MAX_BODY_CHARS} characters.*"
        )
    return body


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--findings", required=True)
    parser.add_argument("--head-sha", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--config-hash", required=True)
    parser.add_argument("--cli-version", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--actor-role", required=True)
    parser.add_argument("--reason", default="")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    # The workflow already validated these. Validate them again here so this
    # script is safe to run by hand and cannot be talked into emitting a body
    # that claims a SHA or a policy hash it did not review.
    if not SHA_RE.match(args.head_sha) or not SHA_RE.match(args.base_sha):
        raise SystemExit("head and base must be 40-character lowercase hex SHAs")
    if not HEX_RE.match(args.config_hash):
        raise SystemExit("config hash must be a 64-character lowercase hex digest")
    if not VERSION_RE.match(args.cli_version):
        raise SystemExit("cli version has an unexpected shape")

    findings, notes = parse_findings(args.findings)
    selected = select(findings, notes)
    body = render(args, selected, notes)

    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump({"commit_id": args.head_sha, "event": "COMMENT", "body": body}, handle)

    counts = {s: sum(1 for f in selected if f["severity"] == s) for s in REPORTED_SEVERITIES}
    print(
        f"rendered {len(selected)} finding(s): "
        f"{counts['critical']} critical, {counts['major']} major, {counts['minor']} minor",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Extractor and expectation evaluator for marked runnable command blocks.

This module holds the pure, hermetic logic behind
``scripts/check-readme-commands.sh``: it parses the marker convention out of a
markdown file, extracts the fenced command blocks the reader is meant to run,
and evaluates each block's declared expectation against a command result. The
shell driver owns everything that touches a live stack (readiness waiting and
running the commands); this module owns everything that can be unit tested with
no network, no docker, and no MinIO. See scripts/test_check_readme_commands.py.

Marker convention (justification)
---------------------------------
A runnable block is marked by an HTML comment on the line IMMEDIATELY before the
opening fence::

    <!-- ravel:run <expectation>[; <expectation> ...] -->
    ```
    curl -s -H "Authorization: Bearer demo-token" http://127.0.0.1:4318/...
    ```

Why an HTML comment: it renders to nothing in GitHub-flavored markdown, so the
README a reader sees is unchanged, yet it is trivially greppable
(``grep -n 'ravel:run'``) and unambiguous. ADR-0081 decision 5 requires an
explicit marker precisely so the executable set stays auditable and an unmarked
block that should have been marked is a reviewable omission rather than a silent
one; a doctest-style "run every fence" approach was rejected there because the
README legitimately contains blocks that must not run (a `cosign verify` naming
a historical tag, a `docker pull` of a moving tag). The ``ravel:`` prefix
namespaces the token so it does not collide with ordinary prose comments.

The marker must sit on the line directly above the fence, with no blank line
between. That rule is what makes the convention unambiguous: a comment floating
elsewhere in the document is not a marker and never runs a block.

Expectation vocabulary (small and closed)
------------------------------------------
Each expectation is one of exactly three keywords. A marker may carry several,
separated by ``;``; all must hold for the block to pass.

    status=<code>       The HTTP status code equals <code>. curl exits 0 on an
                        HTTP 401 or 500, so this is the assertion an exit-code
                        check cannot make. Requires the runner to have captured
                        a status (the shell driver does this for curl commands).

    json:<path>=<value> The command's stdout parses as JSON and the value at
                        <path> equals <value> (compared as text). <path> is a
                        minimal dotted path: `.status`, `.data.resultType`,
                        `.data.result.0`. A `{"status":"error"}` envelope thus
                        fails `json:.status=success` even though the process
                        exited 0 -- the whole point of the gate.

    nonempty:<path>     The command's stdout parses as JSON and the value at
                        <path> is present and non-empty (a non-empty array,
                        object, or string; any number is non-empty).

Fail closed: an empty expectation list, an unparseable marker, or an unknown
keyword raises MarkerError. A block with no expectation is an error, never a
silent skip -- a silent skip is how this class of check rots (ADR-0081 D5).
"""

import argparse
import base64
import json
import sys


class MarkerError(Exception):
    """A marker or expectation that cannot be parsed. Always fatal, never a skip."""


class Block:
    """A marked, runnable command block extracted from a markdown file."""

    __slots__ = ("line", "command", "expectations", "raw_expectation")

    def __init__(self, line, command, expectations, raw_expectation):
        self.line = line  # 1-indexed source line of the opening fence
        self.command = command  # verbatim inner text of the fence
        self.expectations = expectations  # list of dicts, see parse_expectations
        self.raw_expectation = raw_expectation

    def __repr__(self):
        return f"Block(line={self.line}, expectations={self.expectations!r})"


_MARKER_PREFIX = "<!--"
_MARKER_TOKEN = "ravel:run"


def _marker_expectation_text(line):
    """Return the expectation text if `line` is a run marker, else None.

    Recognizes `<!-- ravel:run ... -->` with arbitrary surrounding whitespace.
    """
    stripped = line.strip()
    if not stripped.startswith(_MARKER_PREFIX) or not stripped.endswith("-->"):
        return None
    inner = stripped[len(_MARKER_PREFIX) : -len("-->")].strip()
    if not inner.startswith(_MARKER_TOKEN):
        return None
    return inner[len(_MARKER_TOKEN) :].strip()


def _is_fence(line):
    return line.lstrip().startswith("```")


def parse_expectations(raw):
    """Parse the marker's expectation text into a list of expectation dicts.

    Raises MarkerError on an empty list, an unknown keyword, or a malformed
    expectation. Never returns an empty list.
    """
    if raw is None or not raw.strip():
        raise MarkerError("marker carries no expectation (fail closed, not a skip)")
    out = []
    for piece in raw.split(";"):
        token = piece.strip()
        if not token:
            raise MarkerError(f"empty expectation in marker {raw!r}")
        out.append(_parse_one_expectation(token))
    if not out:
        raise MarkerError(f"marker {raw!r} produced no expectations")
    return out


def _parse_one_expectation(token):
    if token.startswith("status="):
        code = token[len("status=") :].strip()
        if not code.isdigit():
            raise MarkerError(f"status= expects an integer code, got {token!r}")
        return {"kind": "status", "code": int(code)}
    if token.startswith("json:"):
        rest = token[len("json:") :]
        if "=" not in rest:
            raise MarkerError(f"json: expects <path>=<value>, got {token!r}")
        path_text, value = rest.split("=", 1)
        segments = _parse_path(path_text)
        if not segments:
            raise MarkerError(f"json: expects a non-empty path, got {token!r}")
        return {"kind": "json", "path": segments, "value": value}
    if token.startswith("nonempty:"):
        segments = _parse_path(token[len("nonempty:") :])
        if not segments:
            raise MarkerError(f"nonempty: expects a non-empty path, got {token!r}")
        return {"kind": "nonempty", "path": segments}
    raise MarkerError(
        f"unknown expectation keyword in {token!r}; "
        "known: status=, json:<path>=<value>, nonempty:<path>"
    )


def _parse_path(text):
    """Split a dotted JSON path into segments. `.a.b.0` -> ['a', 'b', '0']."""
    return [seg for seg in text.strip().lstrip(".").split(".") if seg != ""]


def extract_blocks(text):
    """Return the ordered list of marked Blocks in `text`.

    Unmarked fenced blocks are ignored (not an error). A marker not immediately
    followed by an opening fence, or a marker with a bad expectation, raises
    MarkerError -- fail closed.
    """
    lines = text.split("\n")
    blocks = []
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        expectation_text = _marker_expectation_text(line)
        if expectation_text is not None:
            marker_lineno = i + 1
            if i + 1 >= n or not _is_fence(lines[i + 1]):
                raise MarkerError(
                    f"line {marker_lineno}: run marker is not immediately "
                    "followed by a code fence"
                )
            expectations = parse_expectations(expectation_text)
            fence_lineno = i + 2
            body, closed_at = _read_fence(lines, i + 1)
            if closed_at is None:
                raise MarkerError(
                    f"line {fence_lineno}: code fence opened but never closed"
                )
            blocks.append(
                Block(
                    line=fence_lineno,
                    command="\n".join(body),
                    expectations=expectations,
                    raw_expectation=expectation_text,
                )
            )
            i = closed_at + 1
            continue
        if _is_fence(line):
            # Unmarked fence: skip its whole body so a `ravel:run` string inside
            # a code sample is never mistaken for a marker.
            _, closed_at = _read_fence(lines, i)
            i = (closed_at + 1) if closed_at is not None else n
            continue
        i += 1
    return blocks


def _read_fence(lines, open_index):
    """Given the index of an opening fence line, return (body_lines, close_index).

    close_index is the index of the closing fence, or None if unterminated.
    """
    body = []
    j = open_index + 1
    while j < len(lines):
        if _is_fence(lines[j]):
            return body, j
        body.append(lines[j])
        j += 1
    return body, None


def _resolve_path(obj, segments):
    """Walk `segments` into a parsed-JSON object. Raises LookupError if absent."""
    cur = obj
    for seg in segments:
        if isinstance(cur, dict):
            if seg not in cur:
                raise LookupError(seg)
            cur = cur[seg]
        elif isinstance(cur, list):
            if not seg.lstrip("-").isdigit():
                raise LookupError(seg)
            idx = int(seg)
            if idx < -len(cur) or idx >= len(cur):
                raise LookupError(seg)
            cur = cur[idx]
        else:
            raise LookupError(seg)
    return cur


def _as_text(value):
    """Render a JSON scalar the way an expectation value is written in a marker."""
    if value is True:
        return "true"
    if value is False:
        return "false"
    if value is None:
        return "null"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _is_empty(value):
    return value is None or value == "" or value == [] or value == {}


def evaluate(expectations, result):
    """Evaluate expectations against a command result.

    `result` is a dict: {"exit": int, "http_status": int|None, "body": str}.
    Returns a list of (ok: bool, detail: str), one per expectation, in order.
    """
    outcomes = []
    parsed_json = _NOT_PARSED
    for exp in expectations:
        kind = exp["kind"]
        if kind == "status":
            outcomes.append(_eval_status(exp, result))
            continue
        # json and nonempty both need the body parsed as JSON, once.
        if parsed_json is _NOT_PARSED:
            parsed_json = _try_parse_json(result.get("body", ""))
        if isinstance(parsed_json, _ParseFailure):
            outcomes.append((False, f"{kind}: stdout is not valid JSON ({parsed_json.msg})"))
            continue
        if kind == "json":
            outcomes.append(_eval_json_equals(exp, parsed_json))
        elif kind == "nonempty":
            outcomes.append(_eval_nonempty(exp, parsed_json))
        else:  # pragma: no cover - parse_expectations guards this
            outcomes.append((False, f"unknown expectation kind {kind!r}"))
    return outcomes


def evaluate_ok(expectations, result):
    """True only if every expectation holds."""
    return all(ok for ok, _ in evaluate(expectations, result))


class _ParseFailure:
    def __init__(self, msg):
        self.msg = msg


_NOT_PARSED = object()


def _try_parse_json(body):
    try:
        return json.loads(body)
    except (ValueError, TypeError) as exc:
        return _ParseFailure(str(exc))


def _eval_status(exp, result):
    got = result.get("http_status")
    want = exp["code"]
    if got is None:
        return (False, f"status: expected {want} but no HTTP status was captured")
    if got == want:
        return (True, f"status={got}")
    return (False, f"status: expected {want}, got {got}")


def _eval_json_equals(exp, parsed):
    path = exp["path"]
    dotted = "." + ".".join(path)
    try:
        value = _resolve_path(parsed, path)
    except LookupError:
        return (False, f"json: path {dotted} is absent")
    got = _as_text(value)
    if got == exp["value"]:
        return (True, f"json {dotted} == {got!r}")
    return (False, f"json: {dotted} expected {exp['value']!r}, got {got!r}")


def _eval_nonempty(exp, parsed):
    path = exp["path"]
    dotted = "." + ".".join(path)
    try:
        value = _resolve_path(parsed, path)
    except LookupError:
        return (False, f"nonempty: path {dotted} is absent")
    if _is_empty(value):
        return (False, f"nonempty: {dotted} is empty")
    return (True, f"nonempty {dotted}")


# --------------------------------------------------------------------------
# CLI: a thin shell-facing wrapper. All logic above is import-testable.
# --------------------------------------------------------------------------


def _cmd_extract(md_path):
    """Emit one NUL-delimited record per marked block for the shell driver.

    Fields per block, each NUL-terminated: line number, raw expectation text,
    base64 of the command (base64 so a multi-line command survives the pipe).
    """
    with open(md_path, "r", encoding="utf-8") as handle:
        text = handle.read()
    blocks = extract_blocks(text)  # MarkerError propagates: fail closed, exit 2
    out = sys.stdout.buffer
    for block in blocks:
        cmd_b64 = base64.b64encode(block.command.encode("utf-8"))
        out.write(str(block.line).encode("ascii") + b"\0")
        out.write(block.raw_expectation.encode("utf-8") + b"\0")
        out.write(cmd_b64 + b"\0")
    out.flush()


def _cmd_evaluate(raw, exit_code, http_text):
    body = sys.stdin.read()
    if http_text is None or http_text == "none":
        http_status = None
    elif http_text.isdigit():
        http_status = int(http_text)
    else:
        print(f"evaluate: bad --http value {http_text!r}", file=sys.stderr)
        return 2
    expectations = parse_expectations(raw)  # MarkerError -> caught in main
    result = {"exit": exit_code, "http_status": http_status, "body": body}
    outcomes = evaluate(expectations, result)
    ok = True
    for passed, detail in outcomes:
        marker = "ok  " if passed else "FAIL"
        print(f"    [{marker}] {detail}")
        ok = ok and passed
    return 0 if ok else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_extract = sub.add_parser("extract", help="emit marked blocks for the shell driver")
    p_extract.add_argument("markdown")

    p_eval = sub.add_parser("evaluate", help="evaluate one block's expectation")
    p_eval.add_argument("expectation")
    p_eval.add_argument("--exit", dest="exit_code", type=int, default=0)
    p_eval.add_argument("--http", dest="http", default="none")

    args = parser.parse_args(argv)
    try:
        if args.command == "extract":
            _cmd_extract(args.markdown)
            return 0
        if args.command == "evaluate":
            return _cmd_evaluate(args.expectation, args.exit_code, args.http)
    except MarkerError as exc:
        print(f"marker error: {exc}", file=sys.stderr)
        return 2
    return 2  # pragma: no cover


if __name__ == "__main__":
    sys.exit(main())

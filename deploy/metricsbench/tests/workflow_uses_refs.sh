#!/bin/sh
# workflow_uses_refs.sh <root> <refs-out> (issue #4099).
#
# Extracts every external `uses:` action reference under
# <root>/.github/workflows/*.yml and <root>/.github/actions/*/*.yml twice, by
# two independent readers, and fails unless both find the same multiset of
# (file, reference) pairs:
#
#   - a line grep for a `uses:` key, the extraction category 3 of
#     every_comparator_pins_an_image_digest.sh has always used;
#   - a structural read of the YAML that takes only jobs.<job>.uses and
#     jobs.<job>.steps[*].uses in a workflow and runs.steps[*].uses in a
#     composite action, wherever the key sits (block or flow mapping, any
#     indentation, quoted or plain).
#
# The cross-check replaces a hand-maintained expected count: a reference one
# reader drops is still found by the other, and the run names it. The
# structural reader is a small python3 stdlib parser for the YAML subset
# workflows use, not PyYAML: the CI job that runs this installs nothing and
# PyYAML is not part of the python3 standard library, and a reader that
# changes with the host's site-packages would not be one check. It refuses
# (exit 2) on any construct outside that subset (anchors, aliases, tags,
# complex keys, more than one document, tab indentation) rather than guess.
#
# A local action (`uses: ./...`) is repo-tracked code and is excluded by both.
#
# Writes the grep side's references to <refs-out> as `file:lineno:ref`, ref
# being the whole trailing content of the line, for the caller's pin check.
#
# Exit 0: both readers agree and found at least one reference. Exit 1: they
# disagree, or neither found any. Exit 2: usage, a missing python3, or a file
# the structural reader could not parse.

set -u

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <root> <refs-out>" >&2
  exit 2
fi
ROOT=$1
REFS_OUT=$2

if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: python3 not found; the structural uses: reader needs it"
  exit 2
fi

GREP_KEYS=$(mktemp)
YAML_OUT=$(mktemp)
YAML_KEYS=$(mktemp)
trap 'rm -f "$GREP_KEYS" "$YAML_OUT" "$YAML_KEYS"' EXIT
: >"$REFS_OUT"
TAB=$(printf '\t')

FILES=$(
  find "$ROOT/.github/workflows" -maxdepth 1 -name '*.yml' -type f 2>/dev/null | sort
  find "$ROOT/.github/actions" -mindepth 2 -maxdepth 2 -name '*.yml' -type f 2>/dev/null | sort
)

# shellcheck disable=SC2086
for wf in $FILES; do
  # A step's `uses:` key appears either on its own line (following a separate
  # `- name:` line) or inline after the list-item dash (`- uses: ...`, for a
  # step with no `name:`). Both forms must match, or every no-name step (most
  # of the `- uses: ./.github/actions/...` and bare `- uses: actions/...`
  # steps in this repo) is silently skipped.
  grep -nE '^[[:space:]]*-?[[:space:]]*uses:[[:space:]]*' "$wf" | while IFS= read -r line; do
    lineno=$(printf '%s\n' "$line" | cut -d: -f1)
    ref=$(printf '%s\n' "$line" | sed -E 's/^[0-9]+:[[:space:]]*-?[[:space:]]*uses:[[:space:]]*//; s/[[:space:]]*$//')
    case "$ref" in
      ./*) continue ;; # local action: repo-tracked code, not a fetched external action
    esac
    echo "$wf:$lineno:$ref" >>"$REFS_OUT"
  done
done

# Comparison key: the file and the reference's first token with one pair of
# surrounding quotes removed, which is what the YAML reader yields for it.
while IFS=: read -r file _ ref; do
  pin=$(printf '%s\n' "$ref" | awk '{print $1}' | sed -E "s/^\"(.*)\"\$/\\1/; s/^'(.*)'\$/\\1/")
  printf '%s\t%s\n' "${file#"$ROOT"/}" "$pin"
done <"$REFS_OUT" | sort >"$GREP_KEYS"

# shellcheck disable=SC2086
python3 - "$ROOT" $FILES >"$YAML_OUT" <<'PY'
import os
import re
import sys


class Refusal(Exception):
    pass


class Scalar(str):
    line = 0


def scalar(text, line):
    s = Scalar(text)
    s.line = line
    return s


def strip_comment(text):
    """Drop a trailing comment: a `#` at the start or after whitespace,
    outside quotes."""
    quote = None
    i = 0
    while i < len(text):
        c = text[i]
        if quote:
            if quote == '"' and c == "\\":
                i += 2
                continue
            if c == quote:
                if quote == "'" and text[i + 1:i + 2] == "'":
                    i += 2
                    continue
                quote = None
        elif c in "\"'" and (i == 0 or text[i - 1] in " \t,[{:"):
            quote = c
        elif c == "#" and (i == 0 or text[i - 1] in " \t"):
            return text[:i].rstrip()
        i += 1
    return text.rstrip()


class Line:
    def __init__(self, number, indent, text):
        self.number = number
        self.indent = indent
        self.text = text


KEY_RE = re.compile(r"""^("(?:[^"\\]|\\.)*"|'(?:[^']|'')*'|[^\s#'"\-?:,\[\]{}&*!|>%@`][^#]*?|-[^\s#][^#]*?)\s*:(?:\s+(.*))?$""")


def split_key(text):
    m = KEY_RE.match(text)
    if not m:
        return None
    key = m.group(1)
    if key[0] in "\"'":
        key = unquote(key, 0)
    return key, (m.group(2) or "")


def unquote(text, line):
    if text[0] == "'":
        if len(text) < 2 or text[-1] != "'":
            raise Refusal("line %d: unterminated single-quoted scalar" % line)
        return text[1:-1].replace("''", "'")
    if len(text) < 2 or text[-1] != '"':
        raise Refusal("line %d: unterminated double-quoted scalar" % line)
    body = text[1:-1]
    if "\\" in body:
        body = body.encode("latin-1", "backslashreplace").decode("unicode_escape")
    return body


class Parser:
    def __init__(self, raw):
        self.raw = raw
        self.lines = []
        for n, text in enumerate(raw, 1):
            body = text.rstrip("\n")
            stripped = body.lstrip(" ")
            if stripped.startswith("\t"):
                raise Refusal("line %d: tab indentation" % n)
            content = strip_comment(stripped)
            if not content:
                continue
            if n == 1 and content == "---":
                continue
            if content in ("---", "...") or content.startswith("%"):
                raise Refusal("line %d: more than one document or a directive" % n)
            self.lines.append(Line(n, len(body) - len(stripped), content))
        self.pos = 0

    def peek(self):
        return self.lines[self.pos] if self.pos < len(self.lines) else None

    def document(self):
        value = self.node(0)
        if self.peek() is not None:
            raise Refusal("line %d: content after the document's root node" % self.peek().number)
        return value

    def node(self, min_indent, seq_at=None):
        ln = self.peek()
        if ln is None:
            return None
        if seq_at is not None and ln.indent == seq_at and is_seq(ln.text):
            return self.sequence(ln.indent)
        if ln.indent < min_indent:
            return None
        if is_seq(ln.text):
            return self.sequence(ln.indent)
        if not ln.text.startswith(("{", "[")) and split_key(ln.text) is not None:
            return self.mapping(ln.indent)
        self.pos += 1
        return self.inline_value(ln.text, ln, ln.indent - 1)

    def sequence(self, indent):
        items = []
        while True:
            ln = self.peek()
            if ln is None or ln.indent != indent or not is_seq(ln.text):
                if ln is not None and ln.indent > indent:
                    raise Refusal("line %d: unexpected indentation in a sequence" % ln.number)
                return items
            rest = ln.text[1:]
            if not rest.strip():
                self.pos += 1
                items.append(self.node(indent + 1))
                continue
            offset = len(rest) - len(rest.lstrip(" "))
            # Re-read the item's content as a line of its own at its column,
            # so `- key: value` opens a mapping at that column.
            self.lines[self.pos] = Line(ln.number, indent + 1 + offset, rest.lstrip(" "))
            items.append(self.node(indent + 1))

    def mapping(self, indent):
        result = {}
        while True:
            ln = self.peek()
            if ln is None or ln.indent < indent:
                return result
            if ln.indent > indent:
                raise Refusal("line %d: unexpected indentation in a mapping" % ln.number)
            if is_seq(ln.text):
                return result
            kv = split_key(ln.text)
            if kv is None:
                raise Refusal("line %d: expected a `key:` line" % ln.number)
            key, value = kv
            if key.startswith("?"):
                raise Refusal("line %d: complex mapping key" % ln.number)
            if key in result:
                raise Refusal("line %d: duplicate key %r" % (ln.number, key))
            self.pos += 1
            if value == "":
                result[key] = self.node(indent + 1, seq_at=indent)
            else:
                result[key] = self.inline_value(value, ln, indent)

    def inline_value(self, value, ln, indent):
        c = value[0]
        if c in "&*!":
            raise Refusal("line %d: anchors, aliases and tags are not read" % ln.number)
        if c in "|>":
            if not re.match(r"^[|>][0-9+\-]*$", value):
                raise Refusal("line %d: block scalar header %r" % (ln.number, value))
            return self.block_scalar(ln, indent)
        if c in "{[":
            text = value
            while not flow_closed(text):
                nxt = self.peek()
                if nxt is None or nxt.indent <= indent:
                    raise Refusal("line %d: unterminated flow collection" % ln.number)
                text += " " + nxt.text
                self.pos += 1
            reader = Flow(text, ln.number)
            out = reader.value()
            reader.skip()
            if reader.i != len(text):
                raise Refusal("line %d: text after a flow collection" % ln.number)
            return out
        text = value
        if c in "\"'":
            while not quote_closed(text):
                nxt = self.peek()
                if nxt is None:
                    raise Refusal("line %d: unterminated quoted scalar" % ln.number)
                text += " " + self.raw[nxt.number - 1].strip()
                self.pos += 1
            return scalar(unquote(text, ln.number), ln.number)
        # A plain scalar continues on following lines indented past its key.
        while True:
            nxt = self.peek()
            if nxt is None or nxt.indent <= indent:
                break
            text += " " + nxt.text
            self.pos += 1
        return scalar(text, ln.number)

    def block_scalar(self, ln, indent):
        # Raw lines, comments included: they are scalar content here.
        body = []
        n = ln.number
        while n < len(self.raw):
            text = self.raw[n].rstrip("\n")
            if text.strip() and len(text) - len(text.lstrip(" ")) <= indent:
                break
            body.append(text)
            n += 1
        while self.pos < len(self.lines) and self.lines[self.pos].number <= n:
            self.pos += 1
        return scalar("\n".join(body), ln.number)


def is_seq(text):
    return text == "-" or text.startswith("- ")


def quote_closed(text):
    q = text[0]
    i = 1
    while i < len(text):
        if q == '"' and text[i] == "\\":
            i += 2
            continue
        if text[i] == q:
            if q == "'" and text[i + 1:i + 2] == "'":
                i += 2
                continue
            if i != len(text) - 1:
                raise Refusal("text after a quoted scalar: %r" % text)
            return True
        i += 1
    return False


def flow_closed(text):
    depth = 0
    quote = None
    i = 0
    while i < len(text):
        c = text[i]
        if quote:
            if quote == '"' and c == "\\":
                i += 1
            elif c == quote:
                quote = None
        elif c in "\"'":
            quote = c
        elif c in "{[":
            depth += 1
        elif c in "}]":
            depth -= 1
            if depth == 0:
                return True
        i += 1
    return False


class Flow:
    def __init__(self, text, line):
        self.text = text
        self.i = 0
        self.line = line

    def skip(self):
        while self.i < len(self.text) and self.text[self.i] in " \t":
            self.i += 1

    def expect(self, c):
        self.skip()
        if self.text[self.i:self.i + 1] != c:
            raise Refusal("line %d: expected %r in a flow collection" % (self.line, c))
        self.i += 1

    def value(self):
        self.skip()
        c = self.text[self.i:self.i + 1]
        if c == "{":
            self.i += 1
            out = {}
            while True:
                self.skip()
                if self.text[self.i:self.i + 1] == "}":
                    self.i += 1
                    return out
                key = self.scalar(key=True)
                if key in out:
                    raise Refusal("line %d: duplicate key %r" % (self.line, key))
                self.expect(":")
                out[str(key)] = self.value()
                self.skip()
                if self.text[self.i:self.i + 1] == ",":
                    self.i += 1
                elif self.text[self.i:self.i + 1] != "}":
                    raise Refusal("line %d: expected ',' or '}' in a flow mapping" % self.line)
        if c == "[":
            self.i += 1
            out = []
            while True:
                self.skip()
                if self.text[self.i:self.i + 1] == "]":
                    self.i += 1
                    return out
                out.append(self.value())
                self.skip()
                if self.text[self.i:self.i + 1] == ",":
                    self.i += 1
                elif self.text[self.i:self.i + 1] != "]":
                    raise Refusal("line %d: expected ',' or ']' in a flow sequence" % self.line)
        return self.scalar(key=False)

    def scalar(self, key):
        self.skip()
        t = self.text
        c = t[self.i:self.i + 1]
        if c in ("&", "*", "!", "?"):
            raise Refusal("line %d: anchors, aliases, tags and complex keys are not read" % self.line)
        if c in ("'", '"'):
            j = self.i + 1
            while j < len(t):
                if c == '"' and t[j] == "\\":
                    j += 2
                    continue
                if t[j] == c:
                    if c == "'" and t[j + 1:j + 2] == "'":
                        j += 2
                        continue
                    break
                j += 1
            if j >= len(t):
                raise Refusal("line %d: unterminated quoted scalar" % self.line)
            out = unquote(t[self.i:j + 1], self.line)
            self.i = j + 1
            return scalar(out, self.line)
        j = self.i
        while j < len(t):
            if t[j] in ",{}[]":
                break
            if t[j] == ":" and (j + 1 == len(t) or t[j + 1] in " \t,{}[]"):
                break
            j += 1
        out = t[self.i:j].strip()
        self.i = j
        if not out and not key:
            return None
        return scalar(out, self.line)


def uses_refs(doc, kind):
    """(line, value) for every uses: the given file kind defines."""
    found = []
    if not isinstance(doc, dict):
        raise Refusal("the document root is not a mapping")

    def steps_of(owner, where):
        steps = owner.get("steps") if isinstance(owner, dict) else None
        if steps is None:
            return
        if not isinstance(steps, list):
            raise Refusal("%s.steps is not a sequence" % where)
        for n, step in enumerate(steps):
            if isinstance(step, dict) and "uses" in step:
                found.append(("%s.steps[%d].uses" % (where, n), step["uses"]))

    if kind == "workflow":
        jobs = doc.get("jobs")
        if not isinstance(jobs, dict):
            raise Refusal("no jobs mapping")
        for name, job in jobs.items():
            if not isinstance(job, dict):
                raise Refusal("jobs.%s is not a mapping" % name)
            if "uses" in job:
                found.append(("jobs.%s.uses" % name, job["uses"]))
            steps_of(job, "jobs.%s" % name)
    else:
        runs = doc.get("runs")
        if isinstance(runs, dict):
            steps_of(runs, "runs")
    out = []
    for where, value in found:
        if not isinstance(value, Scalar):
            raise Refusal("%s is not a scalar" % where)
        if not value.startswith("./"):
            out.append((value.line, str(value)))
    return out


def main():
    root = sys.argv[1]
    failed = False
    for path in sys.argv[2:]:
        rel = os.path.relpath(path, root)
        kind = "action" if rel.startswith(os.path.join(".github", "actions") + os.sep) else "workflow"
        try:
            with open(path, encoding="utf-8") as handle:
                raw = handle.readlines()
            refs = uses_refs(Parser(raw).document(), kind)
        except (Refusal, OSError, UnicodeDecodeError) as err:
            print("REFUSED\t%s\t%s" % (rel, err))
            failed = True
            continue
        for line, value in refs:
            if "\t" in value or "\n" in value:
                print("REFUSED\t%s\tline %d: uses: value spans lines or holds a tab" % (rel, line))
                failed = True
                continue
            print("REF\t%s\t%s" % (rel, value))
    return 2 if failed else 0


sys.exit(main())
PY
yaml_rc=$?

grep '^REFUSED' "$YAML_OUT" | while IFS="$TAB" read -r _ file why; do
  echo "FAIL: the structural uses: reader could not read $file: $why"
done
if [ "$yaml_rc" -ne 0 ]; then
  [ -s "$YAML_OUT" ] || echo "FAIL: the structural uses: reader exited $yaml_rc"
  exit 2
fi
sed -n "s/^REF$TAB//p" "$YAML_OUT" | sort >"$YAML_KEYS"

grep_count=$(wc -l <"$GREP_KEYS" | tr -d '[:space:]')
yaml_count=$(wc -l <"$YAML_KEYS" | tr -d '[:space:]')
echo "  external action references: $grep_count by line grep, $yaml_count by YAML structure"

# Both lists are sorted (file, reference) pairs, compared as multisets: comm
# pairs duplicate lines one for one.
only_grep=$(comm -23 "$GREP_KEYS" "$YAML_KEYS")
only_yaml=$(comm -13 "$GREP_KEYS" "$YAML_KEYS")

rc=0
if [ -n "$only_grep" ]; then
  echo "FAIL: references only the line grep found (not a jobs/steps uses: in the YAML):"
  printf '%s\n' "$only_grep" | sed "s/$TAB/: /; s/^/    /"
  rc=1
fi
if [ -n "$only_yaml" ]; then
  echo "FAIL: references only the YAML structure found (the line grep missed them):"
  printf '%s\n' "$only_yaml" | sed "s/$TAB/: /; s/^/    /"
  rc=1
fi
if [ "$grep_count" -eq 0 ] && [ "$yaml_count" -eq 0 ]; then
  echo "FAIL: neither reader found a workflow action reference; the check must never scan zero actions"
  rc=1
fi
exit "$rc"

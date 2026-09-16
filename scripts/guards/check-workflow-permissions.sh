#!/usr/bin/env bash
# Workflow-permissions guard: every workflow under .github/workflows/ declares
# a top-level `permissions:` block, and that block is a floor rather than a
# grant.
#
#   no-permissions    a workflow with no top-level `permissions:` key. Its jobs
#                     inherit the repository default workflow permission, which
#                     is write here. Every job that runs cargo then puts a
#                     read-write GITHUB_TOKEN in the environment of every
#                     dependency build script in the graph.
#   top-level-write   a top-level block granting a write scope. The floor is
#                     what every job inherits, including one added later that
#                     needs none of it. A job that genuinely needs write
#                     declares it on itself.
#
# Usage:
#   scripts/guards/check-workflow-permissions.sh [path ...]   # default:
#                                                             # .github/workflows
#
# Exit 0 clean, 1 on findings, 64 on bad usage or on a scan that found no
# workflow to check. Findings print as `file:line: rule: explanation`.
#
# Escape hatch for top-level-write only, on the flagged line or anywhere in the
# comment block immediately above the `permissions:` key:
#
#   # workflow-permissions-allow: top-level-write -- <reason>
#
# no-permissions has no escape hatch. A workflow that wants no scope at all
# writes `permissions: {}`, which is a declaration and satisfies the rule.
set -uo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "${repo_root}" || exit 1

roots=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h | --help)
      sed -n '2,30p' "$0"
      exit 0
      ;;
    -*)
      echo "check-workflow-permissions.sh: unknown option: $1" >&2
      exit 64
      ;;
    *)
      roots+=("$1")
      shift
      ;;
  esac
done
if [[ ${#roots[@]} -eq 0 ]]; then
  roots=(.github/workflows)
fi

for root in "${roots[@]}"; do
  if [[ ! -d "${root}" ]]; then
    echo "check-workflow-permissions.sh: no such directory: ${root}" >&2
    exit 64
  fi
done

findings_file="$(mktemp "${TMPDIR:-/tmp}/ravel-workflow-perms.XXXXXX")"
sources_file="$(mktemp "${TMPDIR:-/tmp}/ravel-workflow-perms-src.XXXXXX")"
trap 'rm -f "${findings_file}" "${sources_file}"' EXIT

find "${roots[@]}" -type f \( -name '*.yml' -o -name '*.yaml' \) \
  -print0 | sort -z >"${sources_file}"

# The anchor. A scan with nothing to scan reports a clean tree, which is the
# one result this guard must never produce by accident: a moved workflow
# directory, a renamed root, or a caller passing a path that no longer exists
# would otherwise turn the guard into a no-op that passes everything.
if [[ ! -s "${sources_file}" ]]; then
  echo "check-workflow-permissions.sh: no workflow files under ${roots[*]}" >&2
  echo "  Refusing to report a clean scan: a guard that checks nothing passes" >&2
  echo "  everything. Point it at the workflow directory, or fix the move." >&2
  exit 64
fi

# A workflow file is a YAML mapping, so its top-level keys sit at column 0 and
# nothing nested can. That makes a line matching `^permissions:` the top-level
# key and nothing else, with no YAML parser and no PyYAML dependency: the other
# guards here are bash and coreutils only, and this one runs in the same
# build-free lane.
awk_prog='
function strip_comment(s,   i) {
  # Permission values are plain scalars (read, write, none, read-all,
  # write-all), so a `#` can only open a comment. A prose comment inside the
  # block ("issues: write lives on the report job") must not read as a grant.
  i = index(s, "#")
  if (i > 0) return substr(s, 1, i - 1)
  return s
}
function has_write(s) {
  return s ~ /(^|[^A-Za-z-])write(-all)?([^A-Za-z-]|$)/
}
function allowed(line,   i) {
  # The marker sits on the flagged line, or anywhere in the contiguous comment
  # block immediately above it, so the reason can run as long as it needs to.
  if (index(raw[line], "workflow-permissions-allow: top-level-write") > 0) return 1
  for (i = line - 1; i >= 1; i--) {
    if (raw[i] !~ /^[ \t]*#/) return 0
    if (index(raw[i], "workflow-permissions-allow: top-level-write") > 0) return 1
  }
  return 0
}
function report(rule, line, why) {
  printf "%s:%d: %s: %s\n", curfile, line, rule, why
}
function scan(   i, body, first) {
  if (perm_line == 0) {
    report("no-permissions", 1, "no top-level `permissions:` block: every job inherits the repository default, which is write. Declare the floor the jobs need, usually `contents: read`")
    return
  }
  # The block is the value on the key line plus every following line indented
  # under it. A non-blank line back at column 0 is the next top-level key and
  # ends it.
  body = strip_comment(substr(raw[perm_line], length("permissions:") + 1))
  for (i = perm_line + 1; i <= nlines; i++) {
    if (raw[i] ~ /^[ \t]*$/) continue
    if (raw[i] !~ /^[ \t]/) break
    body = body " " strip_comment(raw[i])
  }
  if (!has_write(body)) return
  if (allowed(perm_line)) return
  report("top-level-write", perm_line, "the workflow-level floor grants a write scope, which every job inherits including ones added later: move the grant onto the job that needs it")
}
FNR == 1 {
  if (nlines > 0) scan()
  delete raw
  nlines = 0; perm_line = 0
  curfile = FILENAME
}
{
  nlines++
  raw[nlines] = $0
  if (perm_line == 0 && $0 ~ /^permissions:/) perm_line = nlines
}
END { if (nlines > 0) scan() }
'

scan_status=0
xargs -0 awk "${awk_prog}" <"${sources_file}" >"${findings_file}" || scan_status=$?
if [[ "${scan_status}" -ne 0 ]]; then
  echo "check-workflow-permissions.sh: the workflow scan failed (exit ${scan_status})." >&2
  echo "  Refusing to report a result: an empty findings file after a failed" >&2
  echo "  scan is indistinguishable from a clean tree." >&2
  exit 70
fi

count=$(grep -c '' "${findings_file}")
if [[ "${count}" -eq 0 ]]; then
  echo "check-workflow-permissions.sh: clean ($(tr -cd '\0' <"${sources_file}" | wc -c | tr -d ' ') workflow files)"
  exit 0
fi

sort -t: -k1,1 -k2,2n "${findings_file}"
echo "check-workflow-permissions.sh: ${count} finding(s)" >&2
exit 1

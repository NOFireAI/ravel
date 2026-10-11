#!/usr/bin/env bash
# assert-struct-literals-current.sh <branch> [base]
# Refuse a branch whose change to a struct's field list or a function's
# parameter list breaks a struct literal or call site that landed on <base>
# after the branch forked. <base> defaults to origin/main; <branch> is any
# revision, and refs/pull/<n>/head is fetched from the base's remote.
#
# Both halves are green on their own. The branch compiles against its own
# base, and the site that landed on main was written against the shape the
# branch is about to change, so the break appears only once the merge queue
# rebases the branch, and costs a CI cycle and a review round to find. PR #2804
# made `ServerConfig.audit_pipeline` an `Option` while #2809 landed a new
# `ServerConfig { .. }` literal written against the old field set (E0308).
# PR #2842 gave `MemoryBudget::set_host_memory_gate` a third argument while
# #2841 landed six two-argument calls in tests (E0061).
#
# What counts as a shape change, read from every `.rs` file the branch changes,
# comparing the merge base with the branch:
#   - a struct with a braced field list (`struct Name {`, any visibility,
#     generics allowed) whose set of fields differs: a field added, removed, or
#     its type or visibility changed. Reordering fields is not a change.
#   - a function or method (`fn name(`, any visibility) whose parameter count
#     or `self` receiver differs. A rename, a struct or function that exists on
#     only one side, and a tuple struct are out of scope.
# Same-named items are pooled across the changed files, so moving an item
# between files is not a change.
#
# Where a site is looked for: `.rs` files added or modified on <base> after
# the merge base that the branch itself does not touch, searched on <base> for
# `Name {` (struct) or `name(` (function; `.name(` and `::name(` included),
# each with a word boundary before the name. Declarations are skipped: `struct
# Name {`, `impl Name {`, `for Name {`, `-> Name {` and `fn name(`.
#
# This is textual and over-approximates on purpose. A `Name {` that is a
# pattern match, or a `name(` that is a different function with the same
# identifier, costs the reader one grep; a miss costs a queue cycle.
#
# <base> of the form <remote>/<branch>, where <remote> is a configured remote,
# is fetched in this invocation with an explicit destination refspec, so a
# stale local remote-tracking ref cannot pass the check. Any other <base> is
# used as it resolves locally.
#
# Exit codes: 0 no shape change, or no post-fork site (one line says what was
# checked); 1 a post-fork site uses a changed shape (each is printed with the
# base commit that brought it in, and the fix); 2 the question could not be
# answered (git failed, an unknown ref); 64 usage.
set -u

usage() {
  echo "usage: assert-struct-literals-current.sh <branch> [base]" >&2
  exit 64
}

case "${1:-}" in
  '' | -h | --help) usage ;;
esac
[ "$#" -le 2 ] || usage

branch_arg=$1
base_arg=${2:-origin/main}

fail() {
  echo "guard: $*" >&2
  exit 2
}

git rev-parse --git-dir >/dev/null 2>&1 || fail "not inside a git repository"

work=$(mktemp -d) || fail "mktemp failed"
tmp_ref=""
# shellcheck disable=SC2329  # invoked via the exit trap below
cleanup() {
  if [ -n "$tmp_ref" ]; then git update-ref -d "$tmp_ref" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup 0
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

is_remote() {
  local r
  for r in $(git remote); do
    [ "$r" = "$1" ] && return 0
  done
  return 1
}

remote=""
base_rev=$base_arg
case "$base_arg" in
  */*)
    candidate=${base_arg%%/*}
    if is_remote "$candidate"; then
      remote=$candidate
      base_branch=${base_arg#*/}
      base_rev="refs/remotes/${remote}/${base_branch}"
      git fetch --quiet -- "$remote" "+refs/heads/${base_branch}:${base_rev}" >/dev/null 2>&1 ||
        fail "git fetch $remote ${base_branch} failed (no such branch on $remote?)"
    fi
    ;;
esac
base_tip=$(git rev-parse --verify --quiet "${base_rev}^{commit}") || fail "cannot resolve base '$base_arg'"

branch_rev=$branch_arg
case "$branch_arg" in
  refs/pull/*)
    [ -n "$remote" ] || remote=origin
    tmp_ref="refs/remotes/${remote}/struct-literals-check-$$"
    git fetch --quiet -- "$remote" "+${branch_arg}:${tmp_ref}" >/dev/null 2>&1 ||
      fail "git fetch $remote ${branch_arg} failed"
    branch_rev=$tmp_ref
    ;;
esac
branch_tip=$(git rev-parse --verify --quiet "${branch_rev}^{commit}") || fail "cannot resolve branch '$branch_arg'"

merge_base=$(git merge-base "$base_tip" "$branch_tip") || fail "no merge base between '$branch_arg' and '$base_arg'"
mb_short=$(git rev-parse --short "$merge_base") || fail "git rev-parse --short failed"

# Emits one line per item: S<TAB>Name<TAB>fields for a braced struct, the
# fields whitespace-stripped and sorted; F<TAB>name<TAB>count|receiver for a
# function signature. Strings, char literals, line comments, one-line
# attributes and arrows are blanked first so their brackets cannot skew the
# depth count.
extract_awk=$(
  cat <<'AWK'
function push_item(   t) {
  t = item
  gsub(/[ \t]/, "", t)
  if (t != "") { n_items++; items[n_items] = t }
  item = ""
}
function emit_struct(   i, j, t, out) {
  for (i = 2; i <= n_items; i++) {
    t = items[i]; j = i - 1
    while (j >= 1 && items[j] > t) { items[j + 1] = items[j]; j-- }
    items[j + 1] = t
  }
  out = ""
  for (i = 1; i <= n_items; i++) out = out (i > 1 ? ";" : "") items[i]
  printf "S\t%s\t%s\n", name, out
}
function emit_fn(   recv, r) {
  recv = "none"
  if (n_items >= 1 && items[1] ~ ("^&?(" q "[A-Za-z_][A-Za-z0-9_]*)?(mut)?self($|:)")) {
    r = items[1]
    sub(/:.*/, "", r)
    if (substr(r, 1, 1) != "&") recv = "self"
    else if (r ~ /mutself$/) recv = "&mut self"
    else recv = "&self"
  }
  printf "F\t%s\t%d|%s\n", name, n_items, recv
}
function find_header(   ss, fs, m) {
  ss = match(rest, /(^|[^A-Za-z0-9_])struct[ \t]+[A-Za-z_][A-Za-z0-9_]*/)
  if (ss) { s_start = RSTART; s_len = RLENGTH }
  fs = match(rest, /(^|[^A-Za-z0-9_])fn[ \t]+[A-Za-z_][A-Za-z0-9_]*/)
  if (fs) { f_start = RSTART; f_len = RLENGTH }
  if (!ss && !fs) return 0
  if (ss && (!fs || s_start < f_start)) {
    m = substr(rest, s_start, s_len); rest = substr(rest, s_start + s_len); mode = "spre"
  } else {
    m = substr(rest, f_start, f_len); rest = substr(rest, f_start + f_len); mode = "fpre"
  }
  sub(/.*(struct|fn)[ \t]+/, "", m)
  name = m; angle = 0; depth = 0; n_items = 0; item = ""
  return 1
}
# One-line `#[...]` and `#![...]` attributes, up to the first `]`. Done by index
# rather than a bracket expression, which awks disagree on.
function strip_attrs(s,   p, e, out) {
  out = ""
  while ((p = index(s, "#[")) || (p = index(s, "#!["))) {
    e = index(substr(s, p), "]")
    if (!e) break
    out = out substr(s, 1, p - 1)
    s = substr(s, p + e)
  }
  return out s
}
# Returns 1 when the item is finished (or abandoned) and the scan is idle again.
function step(c) {
  if (mode == "spre" || mode == "fpre") {
    if (c == "<") { angle++; return 0 }
    if (c == ">") { if (angle > 0) angle--; return 0 }
    if (angle > 0) return 0
    if (mode == "spre" && c == "{") { mode = "sbody"; depth = 1; return 0 }
    if (mode == "fpre" && c == "(") { mode = "fparams"; depth = 1; return 0 }
    if (c == "{" || c == "(" || c == ";") { mode = "idle"; return 1 }
    return 0
  }
  if (c == "{" || c == "(" || c == "[" || c == "<") depth++
  else if (c == "}" || c == ")" || c == "]" || c == ">") {
    depth--
    if (depth == 0) {
      push_item()
      if (mode == "sbody") emit_struct(); else emit_fn()
      mode = "idle"
      return 1
    }
  } else if (c == "," && depth == 1) { push_item(); return 0 }
  item = item c
  return 0
}
BEGIN { mode = "idle"; q = "'" }
{
  line = $0
  gsub(q "[^" q "\\\\]" q, "", line)
  gsub(q "\\\\[^" q "]*" q, "", line)
  gsub(/"([^"\\]|\\.)*"/, "\"\"", line)
  sub(/\/\/.*/, "", line)
  line = strip_attrs(line)
  gsub(/->/, " ", line)
  gsub(/=>/, " ", line)
  rest = line
  while (1) {
    if (mode == "idle" && !find_header()) break
    done = 0
    n = length(rest)
    for (i = 1; i <= n; i++) {
      if (step(substr(rest, i, 1))) { rest = substr(rest, i + 1); done = 1; break }
    }
    if (!done) { if (mode == "sbody" || mode == "fparams") item = item " "; break }
  }
}
AWK
)

# Every file the branch touches, any type: a site in one of these is the
# branch's own business, and its own CI compiled it.
git diff --name-only --no-renames "$merge_base" "$branch_tip" >"$work/branch-files" ||
  fail "git diff --name-only $mb_short..$branch_arg failed"
: >"$work/old-items"
: >"$work/new-items"
while IFS= read -r f; do
  case "$f" in *.rs) ;; *) continue ;; esac
  for side in old new; do
    if [ "$side" = old ]; then rev=$merge_base; else rev=$branch_tip; fi
    # cat-file -e exits 1 for a missing object and 128 for a broken repository.
    rc=0
    git cat-file -e "${rev}:${f}" 2>/dev/null || rc=$?
    [ "$rc" = 0 ] || continue
    git show "${rev}:${f}" >"$work/blob" || fail "git show ${rev}:${f} failed"
    awk "$extract_awk" "$work/blob" >>"$work/${side}-items" || fail "item extraction failed on ${f}"
  done
done <"$work/branch-files"

LC_ALL=C sort "$work/old-items" >"$work/old-sorted" || fail "sort failed"
LC_ALL=C sort "$work/new-items" >"$work/new-sorted" || fail "sort failed"
# Kind and name of every item present on both sides whose pooled shapes differ.
awk -F '\t' -v oldf="$work/old-sorted" '
  { k = $1 "\t" $2; if (FILENAME == oldf) o[k] = o[k] "|" $3; else n[k] = n[k] "|" $3 }
  END { for (k in o) if ((k in n) && o[k] != n[k]) print k }
' "$work/old-sorted" "$work/new-sorted" >"$work/changed-raw" || fail "comparing shapes failed"
LC_ALL=C sort "$work/changed-raw" >"$work/changed" || fail "sort failed"
n_structs=$(awk '/^S/ { n++ } END { print n + 0 }' "$work/changed") || fail "counting structs failed"
n_fns=$(awk '/^F/ { n++ } END { print n + 0 }' "$work/changed") || fail "counting functions failed"

git log --name-only --no-renames --format= "$merge_base..$base_tip" -- '*.rs' >"$work/postfork-raw" ||
  fail "git log $mb_short..$base_arg failed"
# Post-fork .rs files minus the ones the branch touches.
awk 'NR == FNR { touched[$0] = 1; next } $0 != "" && !($0 in touched) && !seen[$0]++' \
  "$work/branch-files" "$work/postfork-raw" >"$work/postfork" || fail "filtering post-fork files failed"
n_files=$(awk 'END { print NR }' "$work/postfork") || fail "counting post-fork files failed"

checked="checked ${n_structs} struct(s) and ${n_fns} function(s) changed on ${branch_arg} against ${n_files} .rs file(s) changed on ${base_arg} since the merge base ${mb_short}"

if [ "$n_structs" = 0 ] && [ "$n_fns" = 0 ] || [ "$n_files" = 0 ]; then
  echo "guard: ${checked}: no stale site"
  exit 0
fi

set --
while IFS="$(printf '\t')" read -r kind item_name; do
  if [ "$kind" = S ]; then
    set -- "$@" -e "(^|[^A-Za-z0-9_])${item_name}[[:space:]]*\\{"
  else
    set -- "$@" -e "(^|[^A-Za-z0-9_])${item_name}[[:space:]]*\\("
  fi
done <"$work/changed"

# git grep exits 1 for no match, which is an answer, and above 1 for an error.
rc=0
git grep -n -I -E "$@" "$base_tip" -- '*.rs' >"$work/grep" 2>"$work/grep-err" || rc=$?
if [ "$rc" -gt 1 ]; then
  fail "git grep on $base_arg failed: $(head -1 "$work/grep-err")"
fi

# One hit per file, kind and name, at its first line: path<TAB>line<TAB>kind<TAB>name.
awk -F '\t' -v tip="$base_tip" -v changedf="$work/changed" -v postf="$work/postfork" '
  FILENAME == changedf { kinds[++nk] = $1; names[nk] = $2; next }
  FILENAME == postf { post[$0] = 1; next }
  {
    rec = substr($0, length(tip) + 2)
    p = index(rec, ":"); file = substr(rec, 1, p - 1); rec = substr(rec, p + 1)
    p = index(rec, ":"); ln = substr(rec, 1, p - 1); text = substr(rec, p + 1)
    if (!(file in post)) next
    for (i = 1; i <= nk; i++) {
      nm = names[i]
      if (kinds[i] == "S") {
        if (text !~ ("(^|[^A-Za-z0-9_])" nm "[[:space:]]*[{]")) continue
        if (text ~ ("(^|[^A-Za-z0-9_])(struct|enum|union|trait|impl|for)[[:space:]]+" nm "[[:space:]]*[{]")) continue
        if (text ~ ("->[[:space:]]*" nm "[[:space:]]*[{]")) continue
      } else {
        if (text !~ ("(^|[^A-Za-z0-9_])" nm "[[:space:]]*[(]")) continue
        if (text ~ ("(^|[^A-Za-z0-9_])fn[[:space:]]+" nm "[[:space:]]*[(]")) continue
      }
      key = file "\t" kinds[i] "\t" nm
      if (!(key in seen)) { seen[key] = 1; print file "\t" ln "\t" kinds[i] "\t" nm }
    }
  }
' "$work/changed" "$work/postfork" "$work/grep" >"$work/hits" || fail "classifying grep hits failed"

if [ ! -s "$work/hits" ]; then
  echo "guard: ${checked}: no stale site"
  exit 0
fi

echo "guard: ${branch_arg} changes a shape that code landed on ${base_arg} after the merge base ${mb_short} still uses:" >&2
while IFS="$(printf '\t')" read -r file ln kind item_name; do
  if [ "$kind" = S ]; then
    what="struct ${item_name} literal"
    pick="(^|[^A-Za-z0-9_])${item_name}[[:space:]]*\\{"
  else
    what="call ${item_name}("
    pick="(^|[^A-Za-z0-9_])${item_name}[[:space:]]*\\("
  fi
  # The newest post-fork commit whose diff adds or removes a matching line,
  # else the newest that touched the file at all.
  commit=$(git log -1 --format='%h %s' -G "$pick" "$merge_base..$base_tip" -- "$file") ||
    fail "git log -G on $file failed"
  if [ -z "$commit" ]; then
    commit=$(git log -1 --format='%h %s' "$merge_base..$base_tip" -- "$file") ||
      fail "git log on $file failed"
  fi
  # A commit subject is attacker-controlled text bound for a terminal.
  commit=$(printf '%s' "$commit" | LC_ALL=C tr -d '\000-\010\013-\037\177')
  echo "guard:   ${file}: ${what} (line ${ln}; changed on ${base_arg} by ${commit})" >&2
done <"$work/hits"
echo "guard: ${checked}" >&2
echo "guard: rebase onto ${base_arg} and update the site" >&2
exit 1

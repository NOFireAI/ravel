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
#     generics and a `where` clause allowed) whose set of fields differs: a
#     field added, removed, or its type or visibility changed. Reordering
#     fields is not a change. A braced enum variant (`Tick { .. }` inside
#     `enum Ev`) is compared the same way, keyed `Ev::Tick`.
#   - a function or method (`fn name(`, any visibility) whose parameter count,
#     `self` receiver, or any parameter's type differs. Renaming a parameter
#     is not a change.
# Out of scope: renaming an item, a struct, variant or function that exists on
# only one side, a tuple struct, and a tuple or unit enum variant.
# Same-named items are pooled across the changed files, so moving an item
# between files is not a change.
#
# Where a site is looked for: `.rs` files added or modified on <base> after
# the merge base that the branch itself does not touch, searched on <base> for
# `Name {` (struct; `Variant {` for a variant, which also finds
# `Enum::Variant {`) or `name(` (function; `.name(` and `::name(` included),
# each with a word boundary before the name. Declarations are skipped: `struct
# Name {`, `impl Name {`, `for Name {`, `-> Name {` and `fn name(`.
#
# A call is reported only when its argument count is one a call written
# against an old shape of the function carries: every parameter for a
# `Type::name(` or bare `name(` call, the parameters less the receiver for a
# `.name(` call (and none for a function without a receiver). The old shapes
# are the ones the branch no longer has, or all of them when it only added
# one. Arguments are counted at bracket depth on the call's own line, with
# strings blanked and a turbofish `::<..>` nested; a call that does not close
# on its line, or has a `|` (a closure) at the top level of its arguments, is
# reported marked "(uncounted)".
#
# This is textual and over-approximates on purpose. A `Name {` that is a
# pattern match, or a `name(` that is a different function with the same
# identifier and an old argument count, costs the reader one grep; a miss
# costs a queue cycle.
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

# Emits one line per item: S<TAB>Name<TAB>fields for a braced struct or a
# braced enum variant (named Enum::Variant), the fields whitespace-stripped and
# sorted; F<TAB>name<TAB>count|receiver|types for a function signature, the
# types of the non-receiver parameters whitespace-stripped and in order, their
# names dropped. Strings, char literals, line comments, one-line
# attributes and arrows are blanked first so their brackets cannot skew the
# depth count.
# Written to a file rather than captured into a variable: bash 3.2 (macOS)
# misparses the quotes of a heredoc that sits inside `$( ... )`.
cat >"$work/extract.awk" <<'AWK'
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
function emit_fn(   recv, r, i, t, types) {
  recv = "none"
  if (n_items >= 1 && items[1] ~ ("^&?(" q "[A-Za-z_][A-Za-z0-9_]*)?(mut)?self($|:)")) {
    r = items[1]
    sub(/:.*/, "", r)
    if (substr(r, 1, 1) != "&") recv = "self"
    else if (r ~ /mutself$/) recv = "&mut self"
    else recv = "&self"
  }
  types = ""
  for (i = 1; i <= n_items; i++) {
    if (i == 1 && recv != "none") continue
    t = items[i]
    sub(/^[^:]*:/, "", t)
    types = types (types == "" ? "" : ";") t
  }
  printf "F\t%s\t%d|%s|%s\n", name, n_items, recv, types
}
function find_header(   ss, fs, es, m, best) {
  best = 0
  ss = match(rest, /(^|[^A-Za-z0-9_])struct[ \t]+[A-Za-z_][A-Za-z0-9_]*/)
  if (ss) { best = RSTART; b_len = RLENGTH; mode = "spre" }
  fs = match(rest, /(^|[^A-Za-z0-9_])fn[ \t]+[A-Za-z_][A-Za-z0-9_]*/)
  if (fs && (!best || RSTART < best)) { best = RSTART; b_len = RLENGTH; mode = "fpre" }
  es = match(rest, /(^|[^A-Za-z0-9_])enum[ \t]+[A-Za-z_][A-Za-z0-9_]*/)
  if (es && (!best || RSTART < best)) { best = RSTART; b_len = RLENGTH; mode = "epre" }
  if (!best) return 0
  m = substr(rest, best, b_len); rest = substr(rest, best + b_len)
  sub(/.*(struct|fn|enum)[ \t]+/, "", m)
  name = m; angle = 0; paren = 0; depth = 0; n_items = 0; item = ""
  if (mode == "epre") ename = m
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
# Inside an enum body only `{`, `(` and `[` nest: at the variant level a `<` is
# a shift in a discriminant, and inside a tuple variant nothing is split.
function step_enum(c,   v) {
  if (c == "{" && depth == 1) {
    v = vtok
    if (match(v, /[A-Za-z_][A-Za-z0-9_]*[ \t]*$/)) {
      v = substr(v, RSTART, RLENGTH)
      gsub(/[ \t]/, "", v)
      name = ename "::" v; mode = "sbody"; in_enum = 1
      depth = 1; n_items = 0; item = ""
      return 0
    }
  }
  if (c == "{" || c == "(" || c == "[") { depth++; return 0 }
  if (c == "}" || c == ")" || c == "]") {
    depth--
    if (depth == 0) { mode = "idle"; return 1 }
    return 0
  }
  if (depth == 1) {
    if (c == ",") vtok = ""
    else vtok = vtok c
  }
  return 0
}
# Returns 1 when the item is finished (or abandoned) and the scan is idle again.
function step(c) {
  if (mode == "ebody") return step_enum(c)
  if (mode == "spre" || mode == "fpre" || mode == "epre") {
    if (c == "<") { angle++; return 0 }
    if (c == ">") { if (angle > 0) angle--; return 0 }
    if (angle > 0) return 0
    if (mode == "fpre") {
      if (c == "(") { mode = "fparams"; depth = 1; return 0 }
      if (c == "{" || c == ";") { mode = "idle"; return 1 }
      return 0
    }
    # A struct or enum header can carry a `where` clause with `Fn(..)` bounds,
    # so parentheses nest here; only `;` (a tuple or unit struct) abandons it.
    if (c == "(") { paren++; return 0 }
    if (c == ")") { if (paren > 0) paren--; return 0 }
    if (paren > 0) return 0
    if (c == "{") {
      if (mode == "spre") { mode = "sbody" } else { mode = "ebody"; vtok = "" }
      depth = 1
      return 0
    }
    if (c == ";") { mode = "idle"; return 1 }
    return 0
  }
  if (c == "{" || c == "(" || c == "[" || c == "<") depth++
  else if (c == "}" || c == ")" || c == "]" || c == ">") {
    depth--
    if (depth == 0) {
      push_item()
      if (mode == "sbody") emit_struct(); else emit_fn()
      if (in_enum) {
        in_enum = 0; mode = "ebody"; name = ename; depth = 1; vtok = ""
        return 0
      }
      mode = "idle"
      return 1
    }
  } else if (c == "," && depth == 1) { push_item(); return 0 }
  item = item c
  return 0
}
BEGIN { mode = "idle"; q = "'"; in_enum = 0 }
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
    if (!done) {
      if (mode == "sbody" || mode == "fparams") item = item " "
      else if (mode == "ebody") vtok = vtok " "
      break
    }
  }
}
AWK

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
    awk -f "$work/extract.awk" "$work/blob" >>"$work/${side}-items" || fail "item extraction failed on ${f}"
  done
done <"$work/branch-files"

LC_ALL=C sort "$work/old-items" >"$work/old-sorted" || fail "sort failed"
LC_ALL=C sort "$work/new-items" >"$work/new-sorted" || fail "sort failed"
# Kind and name of every item present on both sides whose pooled shapes differ.
# A function also gets the argument counts a call written against its old
# shape carries, space-padded: as a path or bare call (every parameter), then
# as a method call (less the receiver; none when there is no receiver). They
# come from the old shapes the branch no longer has, or from every old shape
# when it only added one.
awk -F '\t' -v oldf="$work/old-sorted" '
  {
    k = $1 "\t" $2
    if (FILENAME == oldf) {
      o[k] = o[k] SUBSEP $3; oc[k, $3]++
      if (oc[k, $3] == 1) ol[k, ++on[k]] = $3
    } else {
      n[k] = n[k] SUBSEP $3; nc[k, $3]++
    }
  }
  function add(list, c) { return index(list, " " c " ") ? list : list c " " }
  END {
    for (k in o) {
      if (!(k in n) || o[k] == n[k]) continue
      if (substr(k, 1, 1) != "F") { print k; continue }
      gone = 0
      for (i = 1; i <= on[k]; i++) if (oc[k, ol[k, i]] > nc[k, ol[k, i]]) gone = 1
      pc = " "; mc = " "
      for (i = 1; i <= on[k]; i++) {
        s = ol[k, i]
        if (gone && oc[k, s] <= nc[k, s]) continue
        split(s, a, "|")
        pc = add(pc, a[1])
        if (a[2] != "none") mc = add(mc, a[1] - 1)
      }
      print k "\t" pc "\t" mc
    }
  }
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
while IFS="$(printf '\t')" read -r kind item_name _counts; do
  if [ "$kind" = S ]; then
    # A braced enum variant is keyed Enum::Variant; `Variant {` also finds
    # `Enum::Variant {`.
    set -- "$@" -e "(^|[^A-Za-z0-9_])${item_name##*::}[[:space:]]*\\{"
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

# Arguments of the call whose `(` ends just before s, counted at bracket depth
# like the extractor counts parameters; -1 when they cannot be counted on this
# line: the call does not close on it, or a `|` at the top level (a closure's
# parameters, or `||`) would make its commas ambiguous. Only a turbofish `::<`
# nests angle brackets, since a bare `<` in an argument is a comparison.
cat >"$work/classify.awk" <<'AWK'
function count_args(s,   i, n, c, d, ang, a, cur) {
  gsub(/->|=>/, "  ", s)
  d = 1; ang = 0; a = 0; cur = 0
  n = length(s)
  for (i = 1; i <= n; i++) {
    c = substr(s, i, 1)
    if (c == "(" || c == "[" || c == "{") { d++; cur = 1 }
    else if (c == ")" || c == "]" || c == "}") {
      d--
      if (d == 0) return a + cur
    } else if (c == "<" && i > 2 && substr(s, i - 2, 2) == "::") { ang++; cur = 1 }
    else if (c == ">" && ang > 0) ang--
    else if (d == 1 && ang == 0 && c == ",") { a++; cur = 0 }
    else if (d == 1 && ang == 0 && c == "|") return -1
    else if (c != " " && c != "\t") cur = 1
  }
  return -1
}
# For one grep hit line and one changed function: "" when no call on the line
# carries an argument count of the old shape, "counted" when one does,
# "uncounted" when a call's arguments cannot be counted.
function call_verdict(text, nm, pc, mc,   t, s, at, pre, after, argc, q) {
  q = "'"
  t = text
  gsub(q "[^" q "\\\\]" q, "", t)
  gsub(q "\\\\[^" q "]*" q, "", t)
  gsub(/"([^"\\]|\\.)*"/, "\"\"", t)
  sub(/\/\/.*/, "", t)
  s = t
  while (match(s, "(^|[^A-Za-z0-9_])" nm "[[:space:]]*[(]")) {
    at = (RSTART == 1 && substr(s, 1, length(nm)) == nm) ? 1 : RSTART + 1
    pre = substr(s, 1, at - 1)
    after = substr(s, RSTART + RLENGTH)
    s = after
    if (pre ~ /(^|[^A-Za-z0-9_])fn[[:space:]]+$/) continue
    argc = count_args(after)
    if (argc < 0) return "uncounted"
    if (index((pre ~ /[.][[:space:]]*$/) ? mc : pc, " " argc " ")) return "counted"
  }
  return ""
}
# One hit per file, kind and name, at its first line:
# path<TAB>line<TAB>kind<TAB>name<TAB>note, the note "uncounted" or empty.
  FILENAME == changedf { kinds[++nk] = $1; names[nk] = $2; pcs[nk] = $3; mcs[nk] = $4; next }
  FILENAME == postf { post[$0] = 1; next }
  {
    rec = substr($0, length(tip) + 2)
    p = index(rec, ":"); file = substr(rec, 1, p - 1); rec = substr(rec, p + 1)
    p = index(rec, ":"); ln = substr(rec, 1, p - 1); text = substr(rec, p + 1)
    if (!(file in post)) next
    for (i = 1; i <= nk; i++) {
      nm = names[i]
      note = ""
      if (kinds[i] == "S") {
        sub(/.*::/, "", nm)
        if (text !~ ("(^|[^A-Za-z0-9_])" nm "[[:space:]]*[{]")) continue
        if (text ~ ("(^|[^A-Za-z0-9_])(struct|enum|union|trait|impl|for)[[:space:]]+" nm "[[:space:]]*[{]")) continue
        if (text ~ ("->[[:space:]]*" nm "[[:space:]]*[{]")) continue
      } else {
        v = call_verdict(text, nm, pcs[i], mcs[i])
        if (v == "") continue
        if (v == "uncounted") note = v
      }
      key = file "\t" kinds[i] "\t" names[i]
      if (!(key in seen)) { seen[key] = 1; print file "\t" ln "\t" kinds[i] "\t" names[i] "\t" note }
    }
  }
AWK
awk -F '\t' -v tip="$base_tip" -v changedf="$work/changed" -v postf="$work/postfork" -f "$work/classify.awk" \
  "$work/changed" "$work/postfork" "$work/grep" >"$work/hits" || fail "classifying grep hits failed"

if [ ! -s "$work/hits" ]; then
  echo "guard: ${checked}: no stale site"
  exit 0
fi

echo "guard: ${branch_arg} changes a shape that code landed on ${base_arg} after the merge base ${mb_short} still uses:" >&2
while IFS="$(printf '\t')" read -r file ln kind item_name note; do
  if [ "$kind" = S ]; then
    case "$item_name" in
      *::*) what="variant ${item_name} literal" ;;
      *) what="struct ${item_name} literal" ;;
    esac
    pick="(^|[^A-Za-z0-9_])${item_name##*::}[[:space:]]*\\{"
  else
    what="call ${item_name}("
    if [ -n "$note" ]; then what="${what} (${note})"; fi
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

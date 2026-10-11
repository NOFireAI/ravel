#!/usr/bin/env bash
# Cases for scripts/guards/assert-struct-literals-current.sh.
#
# Every case builds a scratch repository under mktemp -d with a `main`, a
# `feature` branch forked from it, and commits landed on `main` after the fork.
# `origin` points at the repository itself, so the guard's fetch of
# origin/main is real and needs no network. No gh.
#
# Each case names the mutation of the guard it catches:
#   struct field added, literal landed after the fork -> 1, names the file
#     (catches: struct changes not collected, post-fork file list empty)
#   the #2804 shape, a field's type changed to Option -> 1, names commit + fix
#     (catches: only added/removed fields compared, not types)
#   a field removed -> 1 (catches: only additions compared)
#   a field's visibility changed -> 1 (catches: visibility stripped)
#   literal on main before the fork, untouched since -> 0
#     (catches: searching every file on base rather than post-fork ones)
#   both sides add a file, main's version holds the literal -> 1
#     (catches: a file the branch touches skipped outright)
#   the branch edits src/other.rs line 1 and makes Cfg.a an Option, main
#     appends a `Cfg { a: 1 }` literal there (a clean rebase) -> 1, naming the
#     file at its line on main, and the count line names one file searched in
#     full and one overlap file (catches: overlap files skipped; hunk line
#     numbers not advanced or read from the old side; overlap files counted
#     as searched in full)
#   main's added lines in an overlap file hold only a declaration while a
#     pre-fork `Cfg { a: 1 }` sits on an untouched line -> 0 (catches: an
#     overlap file searched in full; declarations not skipped there)
#   the branch widens `fn gate(limit: u64)` and edits src/site.rs, main
#     appends `gate(5)` there -> 1, or `gate(5, 6)` -> 0 (catches: overlap
#     files skipped for calls; the argument-count rule not applied there)
#   fields only reordered -> 0 (catches: comparing field order, not the set)
#   the #2842 shape, a pub fn gains a parameter, six two-argument calls landed
#     after the fork -> 1 (catches: parameter counts not compared; a method
#     call matched against the count with the receiver)
#   a method call spelled `.name(` -> 1, and a path call `Type::name(` -> 1
#     (catches: a search anchored on a bare call or on whitespace before it)
#   the self receiver changes while the count does not -> 1
#     (catches: receivers not compared)
#   a function renamed -> 0 (pins: a rename is out of scope)
#   a tuple struct's fields change -> 0 (pins: tuple structs out of scope)
#   a function moved to another file unchanged -> 0
#     (catches: items compared per file instead of pooled)
#   post-fork file holds only declarations (`impl Name {`, `for Name {`,
#     `-> Name {`, `struct Name {`, `fn name(`) -> 0
#     (catches: declaration lines reported as literals or calls)
#   a `Name { .. } =>` pattern, or a call of a different function sharing
#     the identifier with an old argument count, landed after the fork -> 1
#     (pins the accepted over-approximation)
#   `Vec::new()` against an old `fn new(limit: u64)` -> 0
#     (catches: a call reported without comparing its argument count)
#   `it.new(5)` against an old receiver-less `fn new(limit: u64)` -> 0
#     (catches: a method call matched against a function with no receiver)
#   an unchanged `fn new()` in another touched file beside the changed one,
#     `Vec::new()` landed -> 0 (catches: counts taken from every old shape
#     rather than the ones the branch removed)
#   the branch only adds a second `fn new` shape, `Pool::new(5)` landed -> 1
#     (catches: no counts at all when no old shape was removed)
#   a three-argument call against the old two-argument method -> 0
#     (catches: counts taken from the new shape, or not compared)
#   a call spanning lines -> 1, marked "(uncounted)"
#     (catches: an unclosed call dropped, or reported without the mark)
#   a closure `|a, b, c|` at the top level of the arguments -> 1, marked
#     "(uncounted)" (catches: a closure's commas counted as arguments)
#   a comma inside a string argument -> 0 (catches: strings not blanked)
#   a turbofish `Vec::<u8, A>::new()` argument -> 1, counted
#     (catches: commas inside a turbofish counted)
#   `min(x, y), x < y` as arguments -> 1, counted (catches: commas inside a
#     nested call counted, or a bare `<` treated as an opening bracket)
#   `fn gate(limit: u64)` to `fn gate(limit: Option<u64>)`, `gate(5)`
#     landed -> 1 (catches: parameter types left out of the shape)
#   a parameter renamed only -> 0 (catches: parameter names in the shape)
#   a braced enum variant gains a field, `Ev::Tick { .. }` and a bare
#     `Tick { .. }` landed -> 1 naming `variant Ev::Tick`
#     (catches: enum variants not extracted, or dropped on the search side)
#   the variant literal was on main before the fork -> 0
#     (catches: variant sites searched outside the post-fork files)
#   a tuple variant's fields change -> 0, and a unit variant made braced
#     -> 0 (pins: both out of scope)
#   a struct with `where T: Fn(u64) -> u64` gains a field -> 1
#     (catches: a `(` in the header abandoning the struct)
#   no `.rs` change on the branch -> 0 with the "checked" line
#   base defaulted (no second argument) -> origin/main is used
#   base `main`, not <remote>/<branch> -> resolved locally and checked
#   stale local origin/main -> still 1 (catches: base read without a fetch)
#   refs/pull/<n>/head as the branch -> fetched and checked
#   a non-existent base branch, an unresolvable local base, an unresolvable
#     branch, and a directory that is not a repository -> 2, never 0
#   no argument, three arguments -> 64
#
# Run by hand:   bash scripts/guards/assert-struct-literals-current.test.sh
set -euo pipefail

SCRIPT="${SCRIPT:-$(cd "$(dirname "$0")" && pwd)/assert-struct-literals-current.sh}"
pass=0
fail=0
ROOT="$(mktemp -d)"
trap 'rm -rf "${ROOT}"' EXIT

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

check_contains() {
  local label="$1" needle="$2" hay="$3"
  if printf '%s' "${hay}" | grep -qF -- "${needle}"; then
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1)); printf 'FAIL  %s\n  missing: %s\n  in: %s\n' "${label}" "${needle}" "${hay}"
  fi
}

check_absent() {
  local label="$1" needle="$2" hay="$3"
  if printf '%s' "${hay}" | grep -qF -- "${needle}"; then
    fail=$((fail + 1)); printf 'FAIL  %s\n  unexpectedly present: %s\n' "${label}" "${needle}"
  else
    pass=$((pass + 1)); printf 'ok    %s\n' "${label}"
  fi
}

# put <repo> <file> <content>: staged beside the repository and written by the
# next commit_on, after it has checked out the branch the change belongs to.
put() {
  mkdir -p "$(dirname "$1.stage/$2")"
  printf '%s\n' "$3" >"$1.stage/$2"
}

# commit_on <repo> <branch> <message>
commit_on() {
  if [[ "$(git -C "$1" symbolic-ref --short HEAD)" != "$2" ]]; then
    git -C "$1" checkout -q "$2"
  fi
  if [[ -d "$1.stage" ]]; then
    cp -R "$1.stage/." "$1/"
    rm -rf "$1.stage"
  fi
  git -C "$1" add -A
  git -C "$1" commit -qm "$3"
}

# A repository whose main holds a ServerConfig struct and a MemoryBudget
# method, with `feature` forked from it. Prints the directory.
new_repo() {
  local dir
  dir="$(mktemp -d "${ROOT}/case.XXXXXX")"
  git -C "${dir}" init -q -b main
  git -C "${dir}" config user.email t@example.com
  git -C "${dir}" config user.name T
  git -C "${dir}" config commit.gpgsign false
  put "${dir}" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
    pub audit_pipeline: AuditPipeline,
}'
  put "${dir}" src/budget.rs 'impl MemoryBudget {
    pub fn set_host_memory_gate(&self, limit: u64, floor: u64) {}
}'
  put "${dir}" README.md 'fixture'
  commit_on "${dir}" main 'base'
  git -C "${dir}" branch feature
  git -C "${dir}" remote add origin "${dir}"
  printf '%s' "${dir}"
}

# The #2804 change: audit_pipeline becomes an Option.
branch_makes_option() {
  put "$1" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
    pub audit_pipeline: Option<AuditPipeline>,
}'
  commit_on "$1" feature 'feat: make the audit pipeline optional'
}

# The #2842 change: set_host_memory_gate takes a third argument.
branch_adds_param() {
  put "$1" src/budget.rs 'impl MemoryBudget {
    pub fn set_host_memory_gate(&self, limit: u64, floor: u64, headroom: u64) {}
}'
  commit_on "$1" feature 'feat: give the host memory gate a headroom'
}

# run <repo> [args...]: sets rc and out.
run() {
  local dir="$1"
  shift
  rc=0
  out="$(cd "${dir}" && bash "${SCRIPT}" "$@" 2>&1)" || rc=$?
  git -C "${dir}" checkout -q main
}

# --- struct field added on the branch, literal landed on main after the fork ---
r="$(new_repo)"
put "${r}" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
    pub audit_pipeline: AuditPipeline,
    pub tls: bool,
}'
commit_on "${r}" feature 'feat: add tls'
put "${r}" src/server/boot.rs 'fn boot() {
    let c = ServerConfig { listen: x, audit_pipeline: p };
}'
commit_on "${r}" main 'feat: boot the server'
run "${r}" feature origin/main
check_eq       "field added + post-fork literal: exits 1" "1" "${rc}"
check_contains "field added + post-fork literal: names the file" \
  "src/server/boot.rs: struct ServerConfig literal (line 2;" "${out}"
check_contains "field added + post-fork literal: prints the fix" \
  "guard: rebase onto origin/main and update the site" "${out}"

# --- the #2804 instance by shape: a field's type changed to Option ---
r="$(new_repo)"
branch_makes_option "${r}"
put "${r}" src/server/audit.rs 'pub fn build() -> Server {
    let cfg = ServerConfig {
        listen: "0.0.0.0:9000".to_string(),
        audit_pipeline: AuditPipeline::default(),
    };
}'
commit_on "${r}" main 'feat(server): audit wiring (#2809)'
landed="$(git -C "${r}" rev-parse --short main)"
run "${r}" feature origin/main
issue_2804_out="${out}"
check_eq       "#2804 shape: exits 1" "1" "${rc}"
check_contains "#2804 shape: names the file, the struct and the landing commit" \
  "src/server/audit.rs: struct ServerConfig literal (line 2; changed on origin/main by ${landed} feat(server): audit wiring (#2809))" "${out}"
check_contains "#2804 shape: says what it checked" \
  "guard: checked 1 struct(s) and 0 function(s) changed on feature against 1 .rs file(s)" "${out}"

# --- a field removed ---
r="$(new_repo)"
put "${r}" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
}'
commit_on "${r}" feature 'refactor: drop the audit pipeline'
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature
check_eq "field removed: exits 1" "1" "${rc}"

# --- a field's visibility changed ---
r="$(new_repo)"
put "${r}" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
    audit_pipeline: AuditPipeline,
}'
commit_on "${r}" feature 'refactor: hide the audit pipeline'
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature
check_eq "field visibility changed: exits 1" "1" "${rc}"

# --- the literal was on main before the fork and untouched since ---
r="$(new_repo)"
put "${r}" src/server/boot.rs 'fn boot() { let c = ServerConfig { listen: x, audit_pipeline: p }; }'
commit_on "${r}" main 'pre-fork literal'
git -C "${r}" branch -f feature main
branch_makes_option "${r}"
put "${r}" src/other.rs 'fn other() {}'
commit_on "${r}" main 'feat: unrelated post-fork change'
run "${r}" feature origin/main
check_eq       "pre-fork literal: exits 0" "0" "${rc}"
check_contains "pre-fork literal: says it checked one struct and one file, searched in full" \
  "guard: checked 1 struct(s) and 0 function(s) changed on feature against 1 .rs file(s) changed on origin/main since the merge base $(git -C "${r}" rev-parse --short feature~1) (1 searched in full, 0 also touched by the branch and searched on the lines origin/main added): no stale site" "${out}"

# --- both sides add the file; main's version holds the literal ---
# Diffed from the merge base, every line of main's file is an added line.
# Catches: a file the branch touches skipped outright.
r="$(new_repo)"
put "${r}" src/config.rs 'pub struct ServerConfig {
    pub listen: String,
    pub audit_pipeline: Option<AuditPipeline>,
}'
put "${r}" src/server/boot.rs 'fn boot() {}'
commit_on "${r}" feature 'feat: optional audit pipeline'
put "${r}" src/server/boot.rs 'fn boot() { let c = ServerConfig { listen: x, audit_pipeline: p }; }'
commit_on "${r}" main 'feat: boot'
run "${r}" feature origin/main
check_eq       "both sides add the literal's file: exits 1" "1" "${rc}"
check_contains "both sides add the literal's file: names it" \
  "src/server/boot.rs: struct ServerConfig literal (line 1;" "${out}"

# overlap_repo <repo>: main gains a `Cfg` struct, src/other.rs holding
# <other.rs> and src/site.rs holding filler, and `feature` forks there.
overlap_repo() {
  put "$1" src/cfg.rs 'pub struct Cfg {
    pub a: u64,
}
pub fn gate(limit: u64) {}'
  put "$1" src/other.rs "$2"
  put "$1" src/site.rs 'fn site() {}
fn s2() {}
fn s3() {}
fn s4() {}
fn s5() {}'
  commit_on "$1" main 'pre-fork cfg'
  git -C "$1" branch -f feature main
}

# --- the branch and main both touch src/other.rs; main appends a literal ---
# The branch edits line 1 and makes Cfg.a an Option; main changes line 3 and
# appends two lines, the second a literal of the old shape, so the rebase is
# clean and only the merge queue's build sees E0308. The literal is line 7 on
# main, the second added line of a hunk that starts at line 6 on main and at
# line 5 on the merge base.
# Catches: overlapping files skipped; a hunk parser that does not advance the
# line number, or reads the old side's start; the count line lumping the
# overlap file in with the full-search ones.
r="$(new_repo)"
overlap_repo "${r}" 'fn other() {}
fn o2() {}
fn o3() {}
fn o4() {}
fn o5() {}'
put "${r}" src/cfg.rs 'pub struct Cfg {
    pub a: Option<u64>,
}
pub fn gate(limit: u64) {}'
put "${r}" src/other.rs 'fn other() { work() }
fn o2() {}
fn o3() {}
fn o4() {}
fn o5() {}'
commit_on "${r}" feature 'feat: make Cfg.a optional'
put "${r}" src/other.rs 'fn other() {}
fn o2() {}
fn o3() { more() }
fn o4() {}
fn o5() {}
fn helper() {}
fn site() { let c = Cfg { a: 1 }; }'
put "${r}" src/unrelated.rs 'fn unrelated() {}'
commit_on "${r}" main 'feat: build a Cfg'
landed="$(git -C "${r}" rev-parse --short main)"
run "${r}" feature origin/main
overlap_out="${out}"
check_eq       "overlap file, literal appended on main: exits 1" "1" "${rc}"
check_contains "overlap file, literal appended on main: names the file and its line on main" \
  "src/other.rs: struct Cfg literal (line 7; changed on origin/main by ${landed} feat: build a Cfg)" "${out}"
check_contains "overlap file, literal appended on main: the count line names the overlap file" \
  "against 2 .rs file(s) changed on origin/main since the merge base $(git -C "${r}" rev-parse --short feature~1) (1 searched in full, 1 also touched by the branch and searched on the lines origin/main added)" "${out}"
check_eq       "overlap file, literal appended on main: the fixture rebases cleanly" "0" \
  "$(git -C "${r}" merge-tree --write-tree main feature >/dev/null 2>&1 && echo 0 || echo 1)"

# --- main's added lines in an overlap file hold no site; an older one does ---
# The pre-fork literal on line 2 reads stale on main because only the branch
# updated it. Main adds a declaration block, which is not a site either.
# Catches: an overlapping file searched in full rather than on its added
# lines; declarations not skipped on added lines.
r="$(new_repo)"
overlap_repo "${r}" 'fn other() {}
fn pre() { let c = Cfg { a: 1 }; }
fn o3() {}
fn o4() {}
fn o5() {}'
put "${r}" src/cfg.rs 'pub struct Cfg {
    pub a: Option<u64>,
}
pub fn gate(limit: u64) {}'
put "${r}" src/other.rs 'fn other() {}
fn pre() { let c = Cfg { a: Some(1) }; }
fn o3() {}
fn o4() {}
fn o5() {}'
commit_on "${r}" feature 'feat: make Cfg.a optional'
put "${r}" src/other.rs 'fn other() {}
fn pre() { let c = Cfg { a: 1 }; }
fn o3() {}
fn o4() {}
fn o5() {}
impl Cfg {
    fn more(&self) {}
}'
commit_on "${r}" main 'feat: Cfg impl'
run "${r}" feature origin/main
check_eq       "overlap file, no site on main's added lines: exits 0" "0" "${rc}"
check_contains "overlap file, no site on main's added lines: the struct and file were checked" \
  "checked 1 struct(s) and 0 function(s) changed on feature against 1 .rs file(s)" "${out}"

# --- an overlap file and a function: the argument-count rule on added lines ---
# The branch gives gate a second parameter and edits line 1 of src/site.rs;
# main appends a call to src/site.rs. A one-argument call is the old shape, a
# two-argument call the new one.
# Catches: overlapping files skipped for calls; added-line hits reported
# without the argument-count rule.
for call in 'gate(5)' 'gate(5, 6)'; do
  r="$(new_repo)"
  overlap_repo "${r}" 'fn other() {}'
  put "${r}" src/cfg.rs 'pub struct Cfg {
    pub a: u64,
}
pub fn gate(limit: u64, floor: u64) {}'
  put "${r}" src/site.rs 'fn site() { work() }
fn s2() {}
fn s3() {}
fn s4() {}
fn s5() {}'
  commit_on "${r}" feature 'feat: give gate a floor'
  put "${r}" src/site.rs "fn site() {}
fn s2() {}
fn s3() {}
fn s4() {}
fn s5() {}
fn call() { ${call}; }"
  commit_on "${r}" main 'feat: call gate'
  run "${r}" feature origin/main
  if [[ "${call}" == 'gate(5)' ]]; then
    check_eq       "overlap file, ${call} appended on main: exits 1" "1" "${rc}"
    check_contains "overlap file, ${call} appended on main: names the call" \
      "src/site.rs: call gate( (line 6;" "${out}"
  else
    check_eq       "overlap file, ${call} appended on main: exits 0" "0" "${rc}"
    check_contains "overlap file, ${call} appended on main: the function was checked" \
      "checked 0 struct(s) and 1 function(s)" "${out}"
  fi
done

# --- fields only reordered ---
r="$(new_repo)"
put "${r}" src/config.rs 'pub struct ServerConfig {
    pub audit_pipeline: AuditPipeline,
    pub listen: String,
}'
commit_on "${r}" feature 'style: reorder fields'
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature origin/main
check_eq       "reorder only: exits 0" "0" "${rc}"
check_contains "reorder only: counts no changed struct" "checked 0 struct(s) and 0 function(s)" "${out}"

# --- the #2842 instance by shape: a pub fn gains a parameter ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" tests/memory_gate.rs 'fn gate_test() {
    let b = MemoryBudget::new();
    b.set_host_memory_gate(0, 1);
    b.set_host_memory_gate(1, 2);
    b.set_host_memory_gate(2, 3);
    b.set_host_memory_gate(3, 4);
    b.set_host_memory_gate(4, 5);
    b.set_host_memory_gate(5, 6);
}'
commit_on "${r}" main 'test: memory gate cases (#2841)'
landed="$(git -C "${r}" rev-parse --short main)"
run "${r}" feature origin/main
issue_2842_out="${out}"
check_eq       "#2842 shape: exits 1" "1" "${rc}"
check_contains "#2842 shape: a method call .name( is found, with the landing commit" \
  "tests/memory_gate.rs: call set_host_memory_gate( (line 3; changed on origin/main by ${landed} test: memory gate cases (#2841))" "${out}"
check_contains "#2842 shape: counts the function" "checked 0 struct(s) and 1 function(s)" "${out}"

# --- a path call Type::name( ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { MemoryBudget::set_host_memory_gate(b, 0, 1); }'
commit_on "${r}" main 'feat: path call'
run "${r}" feature origin/main
check_eq       "path call: exits 1" "1" "${rc}"
check_contains "path call: names the file" "src/use.rs: call set_host_memory_gate(" "${out}"

# --- the self receiver changes while the parameter count does not ---
r="$(new_repo)"
put "${r}" src/budget.rs 'impl MemoryBudget {
    pub fn set_host_memory_gate(budget: &MemoryBudget, limit: u64, floor: u64) {}
}'
commit_on "${r}" feature 'refactor: associated function'
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(0, 1); }'
commit_on "${r}" main 'feat: method call'
run "${r}" feature origin/main
check_eq "receiver changed, same count: exits 1" "1" "${rc}"

# --- a rename is out of scope ---
r="$(new_repo)"
put "${r}" src/budget.rs 'impl MemoryBudget {
    pub fn set_memory_gate(&self, limit: u64, floor: u64) {}
}'
commit_on "${r}" feature 'refactor: rename'
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(0, 1); }'
commit_on "${r}" main 'feat: call the old name'
run "${r}" feature origin/main
check_eq "rename: exits 0 (out of scope)" "0" "${rc}"

# --- a tuple struct is out of scope ---
r="$(new_repo)"
put "${r}" src/tuple.rs 'pub struct Pair(u32, u64);'
commit_on "${r}" main 'tuple'
git -C "${r}" branch -f feature main
put "${r}" src/tuple.rs 'pub struct Pair(u32, u64, u8);'
commit_on "${r}" feature 'feat: widen pair'
put "${r}" src/use.rs 'fn u() { let p = Pair(1, 2); }'
commit_on "${r}" main 'feat: build a pair'
run "${r}" feature origin/main
check_eq "tuple struct: exits 0 (out of scope)" "0" "${rc}"

# --- a function moved to another file, signature unchanged ---
r="$(new_repo)"
put "${r}" src/budget.rs 'impl MemoryBudget {}'
put "${r}" src/gate.rs 'impl MemoryBudget {
    pub fn set_host_memory_gate(&self, limit: u64, floor: u64) {}
}'
commit_on "${r}" feature 'refactor: move the gate'
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(0, 1); }'
commit_on "${r}" main 'feat: call'
run "${r}" feature origin/main
check_eq "moved between files: exits 0" "0" "${rc}"

# --- declarations are not literals or calls ---
r="$(new_repo)"
branch_makes_option "${r}"
branch_adds_param "${r}"
put "${r}" src/impls.rs 'impl ServerConfig {
    pub fn fresh() -> ServerConfig {
        todo()
    }
}
impl Default for ServerConfig {
}
mod legacy {
    pub struct ServerConfig {
        pub listen: String,
    }
    impl Other {
        pub fn set_host_memory_gate(&self, limit: u64) {}
    }
}'
commit_on "${r}" main 'feat: impls'
run "${r}" feature origin/main
check_eq       "declarations only: exits 0" "0" "${rc}"
check_contains "declarations only: both items were checked" "checked 1 struct(s) and 1 function(s)" "${out}"

# --- the accepted over-approximation: a pattern match counts as a literal ---
r="$(new_repo)"
branch_makes_option "${r}"
put "${r}" src/matcher.rs 'fn m(c: &ServerConfig) {
    match c { ServerConfig { listen, .. } => {} }
}'
commit_on "${r}" main 'feat: match'
run "${r}" feature origin/main
check_eq "pattern match: exits 1 (accepted over-count)" "1" "${rc}"

# --- the accepted over-approximation: a different function, same identifier ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/other.rs 'fn o(t: &Throttle) { t.set_host_memory_gate(5, 6); }'
commit_on "${r}" main 'feat: throttle gate'
run "${r}" feature origin/main
check_eq "same identifier, other function: exits 1 (accepted over-count)" "1" "${rc}"

# fork_with <repo> <file> <old> <new>: main gains <old>, feature forks there
# and changes it to <new>.
fork_with() {
  put "$1" "$2" "$3"
  commit_on "$1" main 'pre-fork item'
  git -C "$1" branch -f feature main
  put "$1" "$2" "$4"
  commit_on "$1" feature 'feat: change the item'
}

# --- a call whose argument count fits no old shape is not reported ---
r="$(new_repo)"
fork_with "${r}" src/pool.rs 'impl Pool {
    pub fn new(limit: u64) -> Pool { todo() }
}' 'impl Pool {
    pub fn new(limit: u64, floor: u64) -> Pool { todo() }
}'
put "${r}" src/vec.rs 'fn f() {
    let v: Vec<u8> = Vec::new();
}'
commit_on "${r}" main 'feat: vec'
run "${r}" feature origin/main
check_eq       "Vec::new() against an old fn new(limit): exits 0" "0" "${rc}"
check_contains "Vec::new() against an old fn new(limit): the function was checked" \
  "checked 0 struct(s) and 1 function(s)" "${out}"

# --- a call with the old count against a fn without a receiver, as a method ---
r="$(new_repo)"
fork_with "${r}" src/pool.rs 'impl Pool {
    pub fn new(limit: u64) -> Pool { todo() }
}' 'impl Pool {
    pub fn new(limit: u64, floor: u64) -> Pool { todo() }
}'
put "${r}" src/m.rs 'fn f(it: &Builder) { it.new(5); }'
commit_on "${r}" main 'feat: builder'
run "${r}" feature origin/main
check_eq "method call of a receiver-less old fn: exits 0" "0" "${rc}"

# --- counts come from the old shapes the branch removed, not every old one ---
r="$(new_repo)"
put "${r}" src/empty.rs 'impl Empty {
    pub fn new() -> Empty { todo() }
}'
fork_with "${r}" src/pool.rs 'impl Pool {
    pub fn new(limit: u64) -> Pool { todo() }
}' 'impl Pool {
    pub fn new(limit: u64, floor: u64) -> Pool { todo() }
}'
put "${r}" src/empty.rs 'impl Empty {
    pub fn new() -> Empty { Empty }
}'
commit_on "${r}" feature 'refactor: empty body'
put "${r}" src/vec.rs 'fn f() { let v: Vec<u8> = Vec::new(); }'
commit_on "${r}" main 'feat: vec'
run "${r}" feature origin/main
check_eq "an unchanged fn new() beside the changed one: Vec::new() exits 0" "0" "${rc}"

# --- a branch that only adds a shape: every old shape counts ---
r="$(new_repo)"
fork_with "${r}" src/pool.rs 'impl Pool {
    pub fn new(limit: u64) -> Pool { todo() }
}' 'impl Pool {
    pub fn new(limit: u64) -> Pool { todo() }
}
impl Lane {
    pub fn new(limit: u64, floor: u64) -> Lane { todo() }
}'
put "${r}" src/p.rs 'fn f() { let p = Pool::new(5); }'
commit_on "${r}" main 'feat: pool'
run "${r}" feature origin/main
check_eq       "shape only added: an old-count call exits 1" "1" "${rc}"
check_contains "shape only added: names the call" "src/p.rs: call new( (line 1;" "${out}"

# --- an argument count matching the new shape is not reported ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(0, 1, 2); }'
commit_on "${r}" main 'feat: three-argument call'
run "${r}" feature origin/main
check_eq "three-argument call against an old two-argument method: exits 0" "0" "${rc}"

# --- a call spanning lines cannot be counted and is reported ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) {
    b.set_host_memory_gate(
        0,
        1,
    );
}'
commit_on "${r}" main 'feat: wrapped call'
run "${r}" feature origin/main
check_eq       "call spanning lines: exits 1" "1" "${rc}"
check_contains "call spanning lines: marked uncounted" \
  "src/use.rs: call set_host_memory_gate( (uncounted) (line 2;" "${out}"

# --- a closure at the top level of the arguments cannot be counted ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(|a, b, c| a, 1); }'
commit_on "${r}" main 'feat: closure argument'
run "${r}" feature origin/main
check_eq       "closure argument: exits 1" "1" "${rc}"
check_contains "closure argument: marked uncounted" "call set_host_memory_gate( (uncounted)" "${out}"

# --- commas in a string, a turbofish and a comparison ---
r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate("a, b"); }'
commit_on "${r}" main 'feat: string argument'
run "${r}" feature origin/main
check_eq "a comma inside a string argument: exits 0" "0" "${rc}"

r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { MemoryBudget::set_host_memory_gate(b, Vec::<u8, A>::new(), 1); }'
commit_on "${r}" main 'feat: turbofish argument'
run "${r}" feature origin/main
check_eq       "turbofish argument: exits 1" "1" "${rc}"
check_absent   "turbofish argument: counted" "(uncounted)" "${out}"

r="$(new_repo)"
branch_adds_param "${r}"
put "${r}" src/use.rs 'fn u(b: &MemoryBudget) { b.set_host_memory_gate(min(x, y), x < y); }'
commit_on "${r}" main 'feat: comparison argument'
run "${r}" feature origin/main
check_eq     "comparison argument: exits 1" "1" "${rc}"
check_absent "comparison argument: counted" "(uncounted)" "${out}"

# --- a parameter type changed, count unchanged ---
r="$(new_repo)"
fork_with "${r}" src/gate.rs 'pub fn gate(limit: u64) {}' 'pub fn gate(limit: Option<u64>) {}'
put "${r}" src/use.rs 'fn u() { gate(5); }'
commit_on "${r}" main 'feat: gate'
run "${r}" feature origin/main
check_eq       "parameter type changed: exits 1" "1" "${rc}"
check_contains "parameter type changed: names the call" "src/use.rs: call gate( (line 1;" "${out}"

# --- a parameter renamed only ---
r="$(new_repo)"
fork_with "${r}" src/gate.rs 'pub fn gate(limit: u64) {}' 'pub fn gate(mut max: u64) {}'
put "${r}" src/use.rs 'fn u() { gate(5); }'
commit_on "${r}" main 'feat: gate'
run "${r}" feature origin/main
check_eq       "parameter renamed only: exits 0" "0" "${rc}"
check_contains "parameter renamed only: no changed function" "checked 0 struct(s) and 0 function(s)" "${out}"

# --- a braced enum variant gains a field ---
r="$(new_repo)"
fork_with "${r}" src/ev.rs 'pub enum Ev {
    Tick { at: u64 },
    Stop,
}' 'pub enum Ev {
    Tick { at: u64, seq: u32 },
    Stop,
}'
put "${r}" src/use.rs 'fn u() { let e = Ev::Tick { at: 1 }; }'
put "${r}" src/bare.rs 'use Ev::*;
fn b() { let e = Tick { at: 2 }; }'
commit_on "${r}" main 'feat: tick'
run "${r}" feature origin/main
check_eq       "enum variant field added: exits 1" "1" "${rc}"
check_contains "enum variant field added: names the variant" \
  "src/use.rs: variant Ev::Tick literal (line 1;" "${out}"
check_contains "enum variant field added: a bare Variant { literal is found" \
  "src/bare.rs: variant Ev::Tick literal (line 2;" "${out}"

# --- the variant literal was on main before the fork ---
r="$(new_repo)"
put "${r}" src/use.rs 'fn u() { let e = Ev::Tick { at: 1 }; }'
fork_with "${r}" src/ev.rs 'pub enum Ev {
    Tick { at: u64 },
}' 'pub enum Ev {
    Tick { at: u64, seq: u32 },
}'
put "${r}" src/other.rs 'fn other() {}'
commit_on "${r}" main 'feat: unrelated'
run "${r}" feature origin/main
check_eq       "pre-fork variant literal: exits 0" "0" "${rc}"
check_contains "pre-fork variant literal: the variant was checked" "checked 1 struct(s)" "${out}"

# --- a tuple variant is out of scope ---
r="$(new_repo)"
fork_with "${r}" src/ev.rs 'pub enum Ev {
    Tock(u8),
}' 'pub enum Ev {
    Tock(u8, u8),
}'
put "${r}" src/use.rs 'fn u() { let e = Ev::Tock(1); }'
commit_on "${r}" main 'feat: tock'
run "${r}" feature origin/main
check_eq "tuple variant: exits 0 (out of scope)" "0" "${rc}"

# --- a unit variant that becomes braced is out of scope ---
r="$(new_repo)"
fork_with "${r}" src/ev.rs 'pub enum Ev {
    Stop,
}' 'pub enum Ev {
    Stop { at: u64 },
}'
put "${r}" src/use.rs 'fn u() { let e = Ev::Stop; }'
commit_on "${r}" main 'feat: stop'
run "${r}" feature origin/main
check_eq "unit variant made braced: exits 0 (out of scope)" "0" "${rc}"

# --- a struct whose where clause carries parentheses ---
r="$(new_repo)"
fork_with "${r}" src/mapper.rs 'pub struct Mapper<T>
where
    T: Fn(u64) -> u64,
{
    pub f: T,
}' 'pub struct Mapper<T>
where
    T: Fn(u64) -> u64,
{
    pub f: T,
    pub g: T,
}'
put "${r}" src/use.rs 'fn u() { let m = Mapper { f: id }; }'
commit_on "${r}" main 'feat: mapper'
run "${r}" feature origin/main
check_eq       "where clause with Fn(..): exits 1" "1" "${rc}"
check_contains "where clause with Fn(..): names the literal" "src/use.rs: struct Mapper literal" "${out}"

# --- no .rs change on the branch ---
r="$(new_repo)"
put "${r}" README.md 'fixture, edited'
commit_on "${r}" feature 'docs: edit'
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature origin/main
check_eq "no .rs change: exits 0" "0" "${rc}"
check_contains "no .rs change: prints the checked line" \
  "guard: checked 0 struct(s) and 0 function(s) changed on feature against 1 .rs file(s) changed on origin/main since the merge base" "${out}"

# --- base defaults to origin/main, fetched in this run ---
r="$(new_repo)"
branch_makes_option "${r}"
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature
check_eq       "default base: exits 1" "1" "${rc}"
check_contains "default base: is origin/main" "rebase onto origin/main and update the site" "${out}"
check_eq       "default base: origin/main was fetched to main's tip" \
  "$(git -C "${r}" rev-parse main)" "$(git -C "${r}" rev-parse refs/remotes/origin/main)"

# --- a base that is not <remote>/<branch> is used as it resolves locally ---
r="$(new_repo)"
branch_makes_option "${r}"
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature main
check_eq       "local base: exits 1" "1" "${rc}"
check_contains "local base: names it in the fix" "rebase onto main and update the site" "${out}"

# --- a stale local origin/main cannot pass the check ---
r="$(new_repo)"
git -C "${r}" update-ref refs/remotes/origin/main main
branch_makes_option "${r}"
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" feature origin/main
check_eq "stale local origin/main: still exits 1" "1" "${rc}"

# --- refs/pull/<n>/head is fetched from the base's remote ---
r="$(new_repo)"
branch_makes_option "${r}"
git -C "${r}" update-ref refs/pull/7/head feature
git -C "${r}" checkout -q main
git -C "${r}" branch -D feature >/dev/null
put "${r}" src/new.rs 'fn f() { ServerConfig { listen: a, audit_pipeline: b }; }'
commit_on "${r}" main 'feat: new'
run "${r}" refs/pull/7/head origin/main
check_eq       "refs/pull head: exits 1" "1" "${rc}"
check_contains "refs/pull head: names the file" "src/new.rs: struct ServerConfig literal" "${out}"
check_eq       "refs/pull head: temporary ref removed" "" \
  "$(git -C "${r}" for-each-ref --format='%(refname)' 'refs/remotes/origin/struct-literals-check-*')"

# --- could not answer: 2, never 0 ---
r="$(new_repo)"
branch_makes_option "${r}"
run "${r}" feature origin/no-such-branch
check_eq       "missing base branch: exits 2" "2" "${rc}"
check_contains "missing base branch: says the fetch failed" "git fetch origin no-such-branch failed" "${out}"
run "${r}" feature no-such-ref
check_eq "unresolvable local base: exits 2" "2" "${rc}"
run "${r}" no-such-branch origin/main
check_eq "unresolvable branch: exits 2" "2" "${rc}"
notrepo="$(mktemp -d "${ROOT}/notrepo.XXXXXX")"
rc=0
out="$(cd "${notrepo}" && GIT_CEILING_DIRECTORIES="${ROOT}" bash "${SCRIPT}" feature 2>&1)" || rc=$?
check_eq "not a repository: exits 2" "2" "${rc}"

# --- usage ---
rc=0
out="$(bash "${SCRIPT}" 2>&1)" || rc=$?
check_eq       "no argument: exits 64" "64" "${rc}"
check_contains "no argument: prints usage" "usage: assert-struct-literals-current.sh <branch> [base]" "${out}"
rc=0
out="$(bash "${SCRIPT}" a b c 2>&1)" || rc=$?
check_eq "three arguments: exits 64" "64" "${rc}"

check_absent "#2804 shape output carries no unrelated call line" "call " "${issue_2804_out}"
check_absent "#2842 shape output carries no struct line" "struct ServerConfig" "${issue_2842_out}"

printf '\n%d passed, %d failed\n' "${pass}" "${fail}"
[[ "${fail}" -eq 0 ]]

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
#   the branch touches the file holding the literal -> 0
#     (catches: branch-touched files not excluded)
#   fields only reordered -> 0 (catches: comparing field order, not the set)
#   the #2842 shape, a pub fn gains a parameter, two-argument calls landed
#     after the fork -> 1 (catches: parameter counts not compared)
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
#     the identifier, landed after the fork -> 1 (pins the accepted
#     over-approximation)
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
check_contains "pre-fork literal: says it checked one struct and one file" \
  "guard: checked 1 struct(s) and 0 function(s) changed on feature against 1 .rs file(s) changed on origin/main since the merge base" "${out}"

# --- the branch itself touches the file holding the post-fork literal ---
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
check_eq "branch touches the literal's file: exits 0" "0" "${rc}"

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
put "${r}" src/other.rs 'fn o(t: &Throttle) { t.set_host_memory_gate(5); }'
commit_on "${r}" main 'feat: throttle gate'
run "${r}" feature origin/main
check_eq "same identifier, other function: exits 1 (accepted over-count)" "1" "${rc}"

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

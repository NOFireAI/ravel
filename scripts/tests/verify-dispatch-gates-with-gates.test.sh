#!/usr/bin/env bash
# Coverage for verify-dispatch-gates.sh's --with-gates mode (issue #1247):
# the flag must run scripts/gates.sh (or its GATES_SH override) exactly
# once from inside the cold worktree and propagate its exit code, the
# receipt path it prints must be the same path fleet-result-merge.sh's
# FLEET_MERGE_SKIP_GATES=1 check computes, and the default (no-flag) mode
# must still run the five hand-listed cargo commands, in order, unchanged.
#
# Pure shell: no cargo build. Everything below runs against throwaway git
# repos under $TMPDIR.
#
# Run: bash scripts/tests/verify-dispatch-gates-with-gates.test.sh
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VERIFY_SCRIPT="${SCRIPT_DIR}/verify-dispatch-gates.sh"

tmproot="$(mktemp -d "${TMPDIR:-/tmp}/verify-dispatch-gates-test.XXXXXX")"
if [[ ! -d "${tmproot}" ]]; then
  echo "FAIL  could not create a temp dir for the scratch repos" >&2
  exit 1
fi
# Physical path, so the paths this test derives compare equal to the ones the
# scripts print via `pwd` and `git rev-parse --git-common-dir`. On macOS
# `$TMPDIR` is a symlink under /var and may carry a trailing slash.
tmproot="$(cd "${tmproot}" && pwd -P)"
trap 'rm -rf "${tmproot}"' EXIT

pass=0
fail=0

check_eq() {
  local label="$1" want="$2" got="$3"
  if [[ "${got}" == "${want}" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n  want: %s\n  got:  %s\n' "${label}" "${want}" "${got}"
  fi
}

check_true() {
  local label="$1" cond="$2"
  if [[ "${cond}" == "1" ]]; then
    pass=$((pass + 1))
    printf 'ok    %s\n' "${label}"
  else
    fail=$((fail + 1))
    printf 'FAIL  %s\n' "${label}"
  fi
}

# new_repo <dir>: a scratch repo with one commit, so `HEAD` resolves.
new_repo() {
  local dir="$1"
  mkdir -p "${dir}"
  git -C "${dir}" init -q -b main
  git -C "${dir}" config user.email "executor@example.test"
  git -C "${dir}" config user.name "Scratch Executor"
  git -C "${dir}" config commit.gpgsign false
  mkdir -p "${dir}/scripts"
  printf 'seed\n' >"${dir}/README.md"
  git -C "${dir}" add README.md
  git -C "${dir}" commit -q -s -m "chore: seed the scratch repo"
}

# --- stub gates.sh --------------------------------------------------------
# Records one line per invocation (cwd, arg count) to CALL_LOG, then either
# writes a receipt mimicking gates.sh's own scheme (tree-hash-keyed file
# under <git-common-dir>/gates-pass/) or exits with STUB_GATES_EXIT.
make_stub_gates() {
  local stub_file="$1"
  cat >"${stub_file}" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
echo "cwd=$(pwd) args=$#" >>"${CALL_LOG}"
if [[ "${STUB_GATES_EXIT:-0}" != "0" ]]; then
  exit "${STUB_GATES_EXIT}"
fi
tree_hash="$(git rev-parse 'HEAD^{tree}')"
receipt_dir="$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass"
mkdir -p "${receipt_dir}"
date -u +%Y-%m-%dT%H:%M:%SZ >"${receipt_dir}/${tree_hash}"
exit 0
EOF
  chmod +x "${stub_file}"
}

# --- stub cargo ------------------------------------------------------------
# Appends its argv (space-joined) as one line to CALL_LOG, then exits 0.
make_stub_cargo() {
  local stub_file="$1"
  cat >"${stub_file}" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${CALL_LOG}"
exit 0
EOF
  chmod +x "${stub_file}"
}

# === (a) --with-gates runs the stub exactly once from inside the worktree,
#     and both a passing (0) and failing (3) exit code are propagated. ====
for exit_code in 0 3; do
  repo="${tmproot}/repo-a-${exit_code}"
  worktree_parent="${tmproot}/wt-a-${exit_code}"
  new_repo "${repo}"
  stub_gates="${tmproot}/stub-gates-${exit_code}.sh"
  make_stub_gates "${stub_gates}"
  call_log="${tmproot}/call-log-a-${exit_code}"
  : >"${call_log}"

  out="$(cd "${repo}" && CALL_LOG="${call_log}" STUB_GATES_EXIT="${exit_code}" \
    GATES_SH="${stub_gates}" "${VERIFY_SCRIPT}" --with-gates HEAD "${worktree_parent}" 2>&1)"
  got_exit=$?

  check_eq "with-gates exit code propagated (stub exit ${exit_code})" "${exit_code}" "${got_exit}"

  call_count="$(wc -l <"${call_log}" | tr -d ' ')"
  check_eq "with-gates stub invoked exactly once (stub exit ${exit_code})" "1" "${call_count}"

  # Prefix, not an exact path: the worktree name carries a per-invocation
  # suffix so two runs of one ref cannot collide, and pinning the old exact
  # shape here would force that suffix back off.
  worktree_prefix="${worktree_parent}/verify-$(cd "${repo}" && git rev-parse --short HEAD)-"
  logged_cwd="$(sed -n 's/^cwd=\(.*\) args=.*/\1/p' "${call_log}")"
  case "${logged_cwd}" in
    "${worktree_prefix}"*) inside="yes" ;;
    *) inside="no (${logged_cwd})" ;;
  esac
  check_eq "with-gates stub ran inside the worktree (stub exit ${exit_code})" "yes" "${inside}"

  # Unscoped, or the real gates.sh writes no receipt: it only stamps one when
  # its crate-argument list is empty, so a scoped invocation here would leave
  # FLEET_MERGE_SKIP_GATES=1 with nothing to find.
  logged_args="$(sed -n 's/^cwd=.* args=\(.*\)/\1/p' "${call_log}")"
  check_eq "with-gates invokes the gate unscoped (stub exit ${exit_code})" "0" "${logged_args}"

  if [[ "${exit_code}" == "0" ]]; then
    printed_receipt="$(printf '%s\n' "${out}" | sed -n 's/^==> Gates receipt: //p')"
    check_true "with-gates prints a receipt path on success" "$([[ -n "${printed_receipt}" ]] && echo 1 || echo 0)"
  fi

  # Worktree must always be removed, pass or fail. Counted by prefix, so any
  # worktree this run created is caught whatever per-invocation suffix it got.
  remaining="$(git -C "${repo}" worktree list --porcelain | grep -c "^worktree ${worktree_prefix}" || true)"
  check_eq "with-gates cleans up the worktree (stub exit ${exit_code})" "0" "${remaining}"
done

# === (b) the printed receipt path matches fleet-result-merge.sh's
#     FLEET_MERGE_SKIP_GATES=1 lookup, run in isolation against the same
#     git common dir. =======================================================
repo="${tmproot}/repo-b"
worktree_parent="${tmproot}/wt-b"
new_repo "${repo}"
stub_gates="${tmproot}/stub-gates-b.sh"
make_stub_gates "${stub_gates}"
call_log="${tmproot}/call-log-b"
: >"${call_log}"

out="$(cd "${repo}" && CALL_LOG="${call_log}" STUB_GATES_EXIT=0 \
  GATES_SH="${stub_gates}" "${VERIFY_SCRIPT}" --with-gates HEAD "${worktree_parent}" 2>&1)"
printed_receipt="$(printf '%s\n' "${out}" | sed -n 's/^==> Gates receipt: //p')"

# fleet-result-merge.sh's own snippet (scripts/fleet-result-merge.sh, the
# FLEET_MERGE_SKIP_GATES=1 branch), reproduced verbatim and run against
# <clean_ref> = HEAD of the same repo the worktree was cut from.
clean_ref="HEAD"
receipt_file="$(
  cd "${repo}"
  skip_tree="$(git rev-parse "${clean_ref}^{tree}")"
  echo "$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass/${skip_tree}"
)"

check_eq "printed receipt path matches fleet-result-merge.sh's lookup" "${receipt_file}" "${printed_receipt}"
check_true "the receipt file the stub wrote actually exists there" "$([[ -f "${receipt_file}" ]] && echo 1 || echo 0)"

# === (b2) VERIFY_WITH_GATES=1 with no flag selects the same gated mode:
#     the gate runs once inside the worktree and the receipt lands where
#     fleet-result-merge.sh looks for it. ====================================
repo="${tmproot}/repo-b2"
worktree_parent="${tmproot}/wt-b2"
new_repo "${repo}"
stub_gates="${tmproot}/stub-gates-b2.sh"
make_stub_gates "${stub_gates}"
call_log="${tmproot}/call-log-b2"
: >"${call_log}"

out="$(cd "${repo}" && CALL_LOG="${call_log}" STUB_GATES_EXIT=0 VERIFY_WITH_GATES=1 \
  GATES_SH="${stub_gates}" "${VERIFY_SCRIPT}" HEAD "${worktree_parent}" 2>&1)"
got_exit=$?

check_eq "VERIFY_WITH_GATES=1 exit code propagated" "0" "${got_exit}"

call_count="$(wc -l <"${call_log}" | tr -d ' ')"
check_eq "VERIFY_WITH_GATES=1 invokes the gate exactly once" "1" "${call_count}"

worktree_prefix="${worktree_parent}/verify-$(cd "${repo}" && git rev-parse --short HEAD)-"
logged_cwd="$(sed -n 's/^cwd=\(.*\) args=.*/\1/p' "${call_log}")"
case "${logged_cwd}" in
  "${worktree_prefix}"*) inside="yes" ;;
  *) inside="no (${logged_cwd})" ;;
esac
check_eq "VERIFY_WITH_GATES=1 runs the gate inside the worktree" "yes" "${inside}"

logged_args="$(sed -n 's/^cwd=.* args=\(.*\)/\1/p' "${call_log}")"
check_eq "VERIFY_WITH_GATES=1 invokes the gate unscoped" "0" "${logged_args}"

printed_receipt="$(printf '%s\n' "${out}" | sed -n 's/^==> Gates receipt: //p')"
receipt_file="$(
  cd "${repo}"
  skip_tree="$(git rev-parse "HEAD^{tree}")"
  echo "$(cd "$(git rev-parse --git-common-dir)" && pwd)/gates-pass/${skip_tree}"
)"
check_eq "VERIFY_WITH_GATES=1 receipt path matches the merge script's lookup" "${receipt_file}" "${printed_receipt}"
check_true "VERIFY_WITH_GATES=1 receipt file exists" "$([[ -f "${receipt_file}" ]] && echo 1 || echo 0)"

# === (c) default mode (no --with-gates) still runs the five cargo
#     commands, in the documented order, unchanged. =========================
repo="${tmproot}/repo-c"
worktree_parent="${tmproot}/wt-c"
new_repo "${repo}"
bin_dir="${tmproot}/bin-c"
mkdir -p "${bin_dir}"
make_stub_cargo "${bin_dir}/cargo"
call_log="${tmproot}/call-log-c"
: >"${call_log}"

(cd "${repo}" && CALL_LOG="${call_log}" PATH="${bin_dir}:${PATH}" \
  "${VERIFY_SCRIPT}" HEAD "${worktree_parent}" >/dev/null 2>&1)
default_exit=$?
check_eq "default mode exits 0 against the stub cargo" "0" "${default_exit}"

want_calls="$(printf '%s\n' \
  '--locked fmt --all --check' \
  'build --locked --workspace --all-targets' \
  'clippy --locked --workspace --all-targets -- -D warnings' \
  'test --locked --workspace' \
  'test --locked --doc --workspace')"
got_calls="$(cat "${call_log}")"
check_eq "default mode invokes the five cargo commands in order" "${want_calls}" "${got_calls}"

# === (d) one run must not delete another run's worktree. ==================
#
# `verify-${short_sha}` collided whenever two runs verified the same ref,
# which is the normal case: a result branch is verified, comes back with a
# finding, and is verified again while the first run is still building. The
# second run's `git worktree add` failed with "already exists" and its EXIT
# trap -- installed before creation and unconditional -- removed the FIRST
# run's worktree, taking an in-flight cold build with it. It surfaced as a
# corrupted gate rather than as a collision.
#
# Mutation: drop the per-invocation suffix from worktree_dir, or set
# created_worktree before `git worktree add` instead of after; either fails
# the first case below.
repo="${tmproot}/repo-d"
worktree_parent="${tmproot}/wt-d"
new_repo "${repo}"
short="$(cd "${repo}" && git rev-parse --short HEAD)"

# A worktree standing in for another run that is mid-build, at the exact path
# the old naming scheme would have chosen.
victim="${worktree_parent}/verify-${short}"
mkdir -p "${worktree_parent}"
git -C "${repo}" worktree add --detach -q "${victim}" HEAD
printf 'in flight\n' >"${victim}/.in-flight"

stub_gates="${tmproot}/stub-gates-d.sh"
make_stub_gates "${stub_gates}"
call_log="${tmproot}/call-log-d"
: >"${call_log}"
(cd "${repo}" && CALL_LOG="${call_log}" STUB_GATES_EXIT="0" \
  GATES_SH="${stub_gates}" "${VERIFY_SCRIPT}" --with-gates HEAD "${worktree_parent}" >/dev/null 2>&1) || true

check_true "a second run leaves the first run's worktree alone" \
  "$([[ -f "${victim}/.in-flight" ]] && echo 1 || echo 0)"
check_true "and git still tracks it" \
  "$(git -C "${repo}" worktree list --porcelain | grep -qc "^worktree ${victim}$" && echo 1 || echo 0)"

# It also cleans up after ITSELF: only the victim is left behind.
own="$(git -C "${repo}" worktree list --porcelain \
  | sed -n "s|^worktree ${worktree_parent}/|&|p" | grep -vc "verify-${short}$" || true)"
check_eq "and removes only the worktree it created" "0" "${own}"

# === (e) an early failure, before the trap is installed, touches nothing. ==
#
# THIS CASE DOES NOT PIN THE OWNERSHIP GUARD. An unresolvable ref fails at
# `git rev-parse` before the EXIT trap is installed and before any worktree
# is created, so `cleanup` never runs and a bystander survives whether or not
# `created_worktree` exists. Verified: with the ownership guard removed
# entirely, this whole suite still reports 25 passed / 0 failed.
#
# What it does check is narrow and still worth a case: an early exit leaves
# the parent directory alone, so a future change that moves the trap above
# the ref resolution, or adds cleanup work to the early-exit path, fails
# here. Case (d) above is what pins the collision fix.
#
# Why the ownership guard has no discriminating test, and why it is not dead
# code, is recorded once beside that guard in verify-dispatch-gates.sh. One
# home to maintain: two copies of a rationale is two things to keep true.
repo="${tmproot}/repo-e"
worktree_parent="${tmproot}/wt-e"
new_repo "${repo}"
mkdir -p "${worktree_parent}"
bystander="${worktree_parent}/bystander"
git -C "${repo}" worktree add --detach -q "${bystander}" HEAD
printf 'keep me\n' >"${bystander}/.keep"

(cd "${repo}" && "${VERIFY_SCRIPT}" "does-not-resolve-as-a-ref" "${worktree_parent}" >/dev/null 2>&1) || true
check_true "an unresolvable ref leaves a bystander worktree intact" \
  "$([[ -f "${bystander}/.keep" ]] && echo 1 || echo 0)"

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]

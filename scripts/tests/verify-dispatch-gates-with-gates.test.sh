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

  worktree_dir="${worktree_parent}/verify-$(cd "${repo}" && git rev-parse --short HEAD)"
  logged_cwd="$(sed -n 's/^cwd=\(.*\) args=.*/\1/p' "${call_log}")"
  check_eq "with-gates stub ran inside the worktree (stub exit ${exit_code})" "${worktree_dir}" "${logged_cwd}"

  if [[ "${exit_code}" == "0" ]]; then
    printed_receipt="$(printf '%s\n' "${out}" | sed -n 's/^==> Gates receipt: //p')"
    check_true "with-gates prints a receipt path on success" "$([[ -n "${printed_receipt}" ]] && echo 1 || echo 0)"
  fi

  # Worktree must always be removed, pass or fail.
  remaining="$(git -C "${repo}" worktree list --porcelain | grep -c "^worktree ${worktree_dir}$" || true)"
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

echo
echo "passed: ${pass}  failed: ${fail}"
[[ ${fail} -eq 0 ]]

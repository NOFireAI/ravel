#!/usr/bin/env bash
# Review one Ravel pull request with the CodeRabbit CLI, locally, as yourself.
#
# This is the fallback path in ADR-0091, and the path to use whenever the
# `coderabbit-oss` GitHub Environment does not exist: if the Open Source plan
# does not grant an Agentic API key, Ravel does not buy one. It reviews with the
# maintainer's own CodeRabbit login instead, which means the allowance spent is
# that maintainer's own and no shared credential or shared automation identity
# exists for a non-maintainer to consume.
#
# It is deliberately not a convenience wrapper:
#
#   * it refuses to run unless GitHub says you hold `maintain` or `admin` on
#     NOFireAI/ravel, checked with the granular `role_name`, because the legacy
#     `permission` field reports a `maintain` user as `write`;
#   * it reads its review policy from `origin/main`, never from the pull
#     request, and passes it to the CLI by absolute path from outside the
#     reviewed tree;
#   * it never builds, tests, installs, sources, or executes anything from the
#     pull request. The tree is inert data;
#   * it prints findings to your terminal. Posting a review to GitHub needs an
#     explicit --publish.
#
# Usage:
#   scripts/coderabbit-maintainer-review.sh 1234
#   scripts/coderabbit-maintainer-review.sh 1234 --publish
#
# Requires: the pinned CodeRabbit CLI on PATH (see "Local maintainer fallback"
# in docs/guides/coderabbit-runbook.md), gh, jq, git, python3.

set -Eeuo pipefail
umask 077

readonly TRUSTED_REPOSITORY="NOFireAI/ravel"
readonly TRUSTED_BASE_BRANCH="main"

die() {
  echo "coderabbit-maintainer-review: $*" >&2
  exit 1
}

pr_number=""
publish=0
reason=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish) publish=1; shift ;;
    --reason)
      shift
      [[ $# -gt 0 ]] || die "--reason requires a value"
      reason="$1"; shift ;;
    -h|--help)
      sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    -*) die "unknown option: $1" ;;
    *)
      [[ -z "$pr_number" ]] || die "expected exactly one pull-request number"
      pr_number="$1"; shift ;;
  esac
done

[[ -n "$pr_number" ]] || die "usage: $0 <pr-number> [--publish] [--reason TEXT]"
[[ "$pr_number" =~ ^[1-9][0-9]{0,5}$ ]] \
  || die "pull-request number must be a positive integer of at most 6 digits"

for tool in gh jq git python3 coderabbit; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool is not on PATH"
done

# macOS ships perl's shasum, most Linux distributions ship coreutils' sha256sum.
if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256; }
else
  die "neither sha256sum nor shasum is on PATH"
fi

repo_root=$(git rev-parse --show-toplevel) || die "run this from inside a Ravel checkout"
cd "$repo_root"

# ---------------------------------------------------------------------------
# 1. The caller must be a maintainer of this repository. Nothing else counts:
#    not organization membership, not CODEOWNERS, not write access.
# ---------------------------------------------------------------------------
me=$(gh api user --jq '.login') || die "gh is not authenticated"
role=""
rc=0
role=$(gh api "repos/${TRUSTED_REPOSITORY}/collaborators/${me}/permission" \
  --jq 'if has("role_name") and (.role_name | type) == "string" then .role_name else "unknown" end') || rc=$?
[[ "$rc" -eq 0 ]] || die "could not establish your effective permission on ${TRUSTED_REPOSITORY}; failing closed"
case "$role" in
  admin|maintain) ;;
  *) die "you are '$role' on ${TRUSTED_REPOSITORY}; maintain or admin is required" ;;
esac
echo "==> $me is $role on ${TRUSTED_REPOSITORY}"

# ---------------------------------------------------------------------------
# 2. Validate the pull request against GitHub, once, and freeze what it says.
# ---------------------------------------------------------------------------
pr_json=$(mktemp) || die "mktemp failed"
trap 'rm -f "$pr_json"' EXIT
gh api "repos/${TRUSTED_REPOSITORY}/pulls/${pr_number}" > "$pr_json" \
  || die "pull request ${pr_number} could not be read from ${TRUSTED_REPOSITORY}"

state=$(jq -r '.state // ""' "$pr_json")
base_repo=$(jq -r '.base.repo.full_name // ""' "$pr_json")
base_ref=$(jq -r '.base.ref // ""' "$pr_json")
head_sha=$(jq -r '.head.sha // ""' "$pr_json")
base_sha=$(jq -r '.base.sha // ""' "$pr_json")
changed=$(jq -r '.changed_files // 0' "$pr_json")

[[ "$state" == "open" ]] || die "pull request is '$state', not open"
[[ "$base_repo" == "$TRUSTED_REPOSITORY" ]] || die "base repository is '$base_repo'"
[[ "$base_ref" == "$TRUSTED_BASE_BRANCH" ]] || die "base branch is '$base_ref', expected $TRUSTED_BASE_BRANCH"
[[ "$head_sha" =~ ^[0-9a-f]{40}$ ]] || die "head SHA is not a commit SHA"
[[ "$base_sha" =~ ^[0-9a-f]{40}$ ]] || die "base SHA is not a commit SHA"
[[ "$changed" -gt 0 ]] || die "pull request has no changed files"
echo "==> PR #${pr_number}: ${changed} changed file(s), head ${head_sha}"

# ---------------------------------------------------------------------------
# 3. Policy comes from origin/main. Not from the working tree, which you may
#    have edited, and never from the pull request.
# ---------------------------------------------------------------------------
git fetch --quiet origin "$TRUSTED_BASE_BRANCH" || die "could not fetch origin/${TRUSTED_BASE_BRANCH}"
policy_dir=$(mktemp -d) || die "mktemp -d failed"
work_dir=$(mktemp -d) || die "mktemp -d failed"
trap 'rm -f "$pr_json"; rm -rf "$policy_dir" "$work_dir"' EXIT

for f in trusted-config.yaml ravel-review-instructions.md render-findings.py cli-pin.env; do
  git show "origin/${TRUSTED_BASE_BRANCH}:.github/coderabbit/${f}" > "${policy_dir}/${f}" \
    || die "could not read .github/coderabbit/${f} from origin/${TRUSTED_BASE_BRANCH}"
done
config_hash=$(cat "${policy_dir}/trusted-config.yaml" "${policy_dir}/ravel-review-instructions.md" \
  | sha256 | cut -d' ' -f1)

# shellcheck disable=SC1091
. "${policy_dir}/cli-pin.env"
installed_version=$(coderabbit --version 2>/dev/null | tr -d '[:space:]') || installed_version=""
if [[ "$installed_version" != "$CODERABBIT_CLI_VERSION" ]]; then
  die "CodeRabbit CLI ${installed_version:-<unknown>} is installed, but this repository pins ${CODERABBIT_CLI_VERSION}.
     ADR-0091 pins the version because the review policy depends on this build's
     --config behaviour. Install the pinned build (see the runbook) or update the
     pin deliberately."
fi

# ---------------------------------------------------------------------------
# 4. Fetch the pull request as inert data, into a temporary directory.
# ---------------------------------------------------------------------------
git init -q "$work_dir"
git -C "$work_dir" config core.hooksPath /dev/null
git -C "$work_dir" config transfer.fsckObjects true
git -C "$work_dir" config fetch.fsckObjects true
git -C "$work_dir" remote add origin "https://github.com/${TRUSTED_REPOSITORY}.git"
git -C "$work_dir" fetch --no-tags --quiet origin \
  "+refs/pull/${pr_number}/head:refs/remotes/origin/pr-head" \
  "+refs/heads/${TRUSTED_BASE_BRANCH}:refs/remotes/origin/base"

fetched_head=$(git -C "$work_dir" rev-parse refs/remotes/origin/pr-head)
[[ "$fetched_head" == "$head_sha" ]] \
  || die "pull-request ref resolved to $fetched_head, not the validated $head_sha"
git -C "$work_dir" cat-file -e "${base_sha}^{commit}" \
  || die "base SHA $base_sha is not reachable from ${TRUSTED_BASE_BRANCH}"
merge_base=$(git -C "$work_dir" merge-base "$base_sha" "$head_sha")
git -C "$work_dir" checkout -q --detach "$head_sha"

# ---------------------------------------------------------------------------
# 5. One review, one head SHA, your own CodeRabbit login. No API key is read
#    from this repository, and none is required to exist.
# ---------------------------------------------------------------------------
findings="${work_dir}.findings.jsonl"
echo "==> reviewing ${head_sha} against ${merge_base} with CodeRabbit ${CODERABBIT_CLI_VERSION}"
rc=0
( cd "$work_dir" && CODERABBIT_CLI_DISABLE_AUTO_UPDATE=1 coderabbit review \
    --base-commit "$merge_base" \
    --agent \
    --config "${policy_dir}/trusted-config.yaml" "${policy_dir}/ravel-review-instructions.md" \
) > "$findings" 2> "${work_dir}.stderr" || rc=$?

if grep -qiE 'rate limit exceeded|too many files|payload_too_large' "${work_dir}.stderr"; then
  rm -f "$findings" "${work_dir}.stderr"
  echo "CodeRabbit declined this review: the plan limit for this hour was reached, or the"
  echo "pull request exceeds the per-review file limit. No retry, no credit, no split."
  exit 0
fi
if [[ "$rc" -ne 0 && ! -s "$findings" ]]; then
  sed -n '1,20p' "${work_dir}.stderr" >&2
  rm -f "$findings" "${work_dir}.stderr"
  die "the CodeRabbit CLI failed (exit $rc) and produced no findings"
fi

review_json="${work_dir}.review.json"
python3 "${policy_dir}/render-findings.py" \
  --findings "$findings" \
  --head-sha "$head_sha" \
  --base-sha "$merge_base" \
  --config-hash "$config_hash" \
  --cli-version "$CODERABBIT_CLI_VERSION" \
  --actor "$me" \
  --actor-role "$role" \
  --reason "$reason" \
  --out "$review_json"

echo
jq -r '.body' "$review_json"
echo

if [[ "$publish" -eq 0 ]]; then
  echo "==> local only. Re-run with --publish to post this as a COMMENT review on PR #${pr_number}."
  rm -f "$findings" "${work_dir}.stderr" "$review_json"
  exit 0
fi

now_head=$(gh api "repos/${TRUSTED_REPOSITORY}/pulls/${pr_number}" --jq '.head.sha')
[[ "$now_head" == "$head_sha" ]] \
  || die "head moved from $head_sha to $now_head while reviewing; refusing to publish a stale review"

gh api --method POST -H "Accept: application/vnd.github+json" \
  "repos/${TRUSTED_REPOSITORY}/pulls/${pr_number}/reviews" \
  --input "$review_json" --jq '"published review \(.id) on PR '"${pr_number}"'"'

rm -f "$findings" "${work_dir}.stderr" "$review_json"

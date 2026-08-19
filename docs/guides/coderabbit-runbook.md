# CodeRabbit administrator runbook

How to turn on, verify, operate, rotate, and remove Ravel's CodeRabbit
integration. The design and its evidence are in
[ADR-0091](../adrs/0091-maintainer-gated-coderabbit-reviews.md); this document
is the operational half, and it covers the controls that cannot be expressed as
files in this repository.

Everything here was verified against CodeRabbit's and GitHub's documentation,
the GitHub API for `NOFireAI/ravel`, and the CodeRabbit CLI binary, on
**2026-08-19**.

Read this first: **the integration is inert until step 3 is done.** No
`coderabbit-oss` environment exists, so the review job cannot start and no
credential exists to spend. That is deliberate. Do not create the environment
until steps 1 and 2 pass.

---

## Who can change what

| Control | Lives in | Who can change it |
|---|---|---|
| CodeRabbit plan, seats, usage-based add-on | CodeRabbit dashboard, billing | CodeRabbit organization owner |
| CodeRabbit organization settings and global overrides | CodeRabbit dashboard, organization settings | CodeRabbit organization owner |
| Agentic API key | CodeRabbit dashboard, API keys | The seat holder the key is issued to |
| GitHub App installation and repository selection | GitHub organization settings, Installed GitHub Apps | GitHub organization owner |
| `coderabbit-oss` environment, its reviewers, its branch policy | Repository settings, Environments | Repository `admin` |
| `CODERABBIT_API_KEY` | The environment above | Repository `admin` |
| `protect-main` ruleset | Repository settings, Rules | Repository `admin` |
| Workflow, trusted policy, CODEOWNERS | This repository, `main` | Repository `admin` via a code-owner-approved pull request |

The seven `admin` holders on `NOFireAI/ravel` today are `ananos`,
`spirosoik`, `pmoust`, `alextoulps`, `safts`, `stylianosrigas`, and
`asapranidis`. Reconcile that list against
`gh api repos/NOFireAI/ravel/collaborators --jq '.[] | select(.role_name == "admin" or .role_name == "maintain") | .login'`
whenever CODEOWNERS or the environment reviewer list is touched.

---

## Step 1: verify the plan, before anything else

Do these in the CodeRabbit dashboard, signed in as an organization owner. Each
one has a stop condition. **If any check fails, stop and go to
[the local fallback](#local-maintainer-fallback). Do not enable billing to make
the integration work.**

| # | What to check | Where | Pass condition |
|---|---|---|---|
| 1.1 | Plan | Subscription page | The organization backing `NOFireAI` shows **Open Source**, not Free, not Pro, not a Pro+ trial, and not "trial ends in N days" |
| 1.2 | Repository treatment | Repository settings for `NOFireAI/ravel` | `NOFireAI/ravel` is listed and receiving OSS treatment, not generic Free-plan treatment |
| 1.3 | Usage-based add-on | Subscription and billing | **Disabled.** The add-on is offered to Pro, Pro+, and Enterprise; a genuine OSS organization should not be able to enable it at all. If it is enabled, the organization is not on OSS |
| 1.4 | Payment method | Billing | No card on file, or a card that no plan draws on. No purchased credits |
| 1.5 | Agentic API key | Settings, API keys | An Agentic API key can be created for this organization **without a charge and without assigning a paid seat** |
| 1.6 | Key organization | The key creation dialog | The key is bound to the organization that owns `NOFireAI/ravel`, since "API-key authentication always uses that key's organization for billing and review behavior" |

Record the date, the plan name shown, and who checked, in the pull request or
issue that enables the integration.

Two traps worth naming.

`NOFireAI` has CodeRabbit installed on private repositories as well. A single
CodeRabbit organization can therefore be on a paid plan even though `ravel`
itself is public. If the organization is on Pro or Pro+ **and** the usage-based
add-on is on, an over-limit CLI review becomes chargeable. Check 1.3 is the one
that catches this, and it is the one most likely to fail.

Check 1.5 is the question this design could not answer from a terminal.
CodeRabbit documents that "the non-interactive API-key flow requires an Agentic
API key from a CodeRabbit organization where the user has an assigned seat", and
does not say whether an Open-Source-plan organization has such a seat. If
creating the key requires purchasing one, that is a paid capability and the
answer is the local fallback.

## Step 2: restrict the GitHub App, or remove it

The App is installed on this organization with repository selection `selected`,
and it has been reviewing Ravel pull requests on its defaults.

**Preferred: remove `NOFireAI/ravel` from the installation.** ADR-0091 does not
use the App for anything, and an uninstalled App has no attack surface to
harden. Organization settings, Installed GitHub Apps, CodeRabbit, Configure,
deselect `ravel`.

Do this only after confirming the one thing that blocks it: whether Open Source
entitlement survives without the installation. The vendor's pricing page says
"install CodeRabbit on a public repository, and receive free reviews forever for
public repositories", which reads as requiring the installation. If OSS
entitlement does require it, keep the App installed for `ravel` only and apply
every restriction below.

Apply these in **CodeRabbit organization settings**, which is the only layer a
pull request cannot edit. A `.coderabbit.yaml` on `main` does not constrain the
review of a pull request that changes it. Where the plan offers global
overrides, mark these as overriding repository configuration.

- Automatic review: **off**
- Automatic incremental review: **off**
- Chat auto-reply: **off**
- Non-organization-member chat: **off**
- Autofix: **off**
- Fix-CI: **off**
- Unit-test generation: **off**
- Docstring generation: **off**
- Merge-conflict resolution: **off**
- Custom finishing touches: **none**
- Pre-merge checks: **off**
- Post-merge actions: **none**
- Issue creation, issue planning, issue enrichment, automatic labelling: **off**
- Jira, Linear, and every external ticket integration: **disabled**
- Knowledge base, learnings, web search, MCP, repository linking: **off**
- Knowledge and instruction sources: restricted to trusted default-branch
  content only

Then **test the restrictions from real accounts.** Settings that were never
exercised are not controls. On a scratch pull request against `main`, from each
identity below, post each command and record what happens:

Identities: anonymous or external fork contributor; outside collaborator;
organization member with `read`; organization member with `triage`;
organization member with `write` (`nofire-bot` and the 13 `write` collaborators
are real examples); a maintainer; an administrator.

Commands: `@coderabbitai review`, `@coderabbitai full review`,
`@coderabbitai autofix`, `@coderabbitai fix-ci`,
`@coderabbitai generate unit tests`, `@coderabbitai resolve`.

A pass requires **every non-maintainer identity to produce no review, no
comment, and no repository mutation.**

If any non-maintainer identity can still cause a review or a mutation, then the
App path does not satisfy Ravel's requirement, and it must not be described as
if it does. Record the result and treat the App as an accepted, documented
residual risk, or remove it. CodeRabbit's finest-grained chat control is
`allow_non_org_members`, an organization-member boundary; `NOFireAI/ravel` has
13 `write` collaborators who are not maintainers, so that boundary is not a
maintainer-only boundary here.

## Step 3: create the protected environment

Only after step 1 passes.

1. Repository settings, Environments, **New environment**, named exactly
   `coderabbit-oss`.
2. **Required reviewers**: add only current `maintain` or `admin` holders. Use
   the reconciliation command in "Who can change what".
3. **Prevent self-review**: on. A maintainer who dispatches the workflow must
   not be able to approve their own deployment.
4. **Deployment branches and tags**: *Selected branches and tags*, one rule,
   exactly `main`. No wildcard, no tag rule.
5. **Custom deployment protection rules**: none. Do not enable a third-party
   protection app here.
6. Add the secret **`CODERABBIT_API_KEY`** as an *environment* secret of
   `coderabbit-oss`. Not a repository secret, not an organization secret, not a
   variable.

Verify:

```sh
gh api repos/NOFireAI/ravel/environments/coderabbit-oss \
  --jq '{name, rules: [.protection_rules[].type], branch_policy: .deployment_branch_policy}'
gh api repos/NOFireAI/ravel/environments/coderabbit-oss/secrets --jq '.secrets[].name'
gh api repos/NOFireAI/ravel/actions/secrets --jq '.secrets[].name'   # must NOT list CODERABBIT_API_KEY
gh api orgs/NOFireAI/actions/secrets --jq '.secrets[].name'          # must NOT list CODERABBIT_API_KEY
```

## Step 4: workflow execution actor rules

GitHub has no setting that limits `workflow_dispatch` to `maintain` and above.
Dispatch comes with `write`. This is why ADR-0091 does not rely on GitHub's
trigger permissions at all, and instead verifies `role_name` at runtime and puts
the credential behind the environment.

Apply what GitHub does offer:

- Repository settings, Actions, General: keep **Allow actions and reusable
  workflows** restricted to what Ravel already uses. This workflow adds no new
  third-party action.
- Workflow permissions: **Read repository contents and packages permissions**.
  The workflow grants what it needs per job and denies the rest with a top-level
  `permissions: {}`.
- If your plan exposes per-workflow execution policies or actor rules, restrict
  `coderabbit-maintainer-review.yml` to `workflow_dispatch` only, restrict the
  permitted actors to the maintainer set, and exclude GitHub Apps, Dependabot,
  and Copilot. Enforce it at organization level so a pull-request branch cannot
  weaken it. Record here whether the feature was available and what was set.
- The workflow itself refuses to run on any ref other than `refs/heads/main`,
  any event other than `workflow_dispatch`, and any repository other than
  `NOFireAI/ravel`. A `write` user who dispatches it is rejected by the
  `role_name` check before the environment is requested.

## Step 5: branch and ruleset protection

**This step is a precondition of the security property, not a nicety.** Today
`protect-main` sets `required_approving_review_count: 0` and
`require_code_owner_review: false`. Under that ruleset a `write` user can open a
pull request editing `.github/workflows/coderabbit-maintainer-review.yml` or
`.github/coderabbit/**` and merge it themselves once CI goes green. That is a
non-maintainer changing the trusted CodeRabbit policy.

Change the `pull_request` rule in the `protect-main` ruleset to:

- `required_approving_review_count`: at least **1**
- `require_code_owner_review`: **true**

Keep the existing `deletion`, `non_fast_forward`, and required status checks.

Verify:

```sh
gh api repos/NOFireAI/ravel/rulesets \
  --jq '.[] | select(.name == "protect-main") | .id' \
  | xargs -I{} gh api repos/NOFireAI/ravel/rulesets/{} \
  --jq '.rules[] | select(.type == "pull_request") | .parameters
        | {required_approving_review_count, require_code_owner_review}'
```

Both must be satisfied. Until they are, `.github/CODEOWNERS` only *requests*
review of the trusted paths and does not require it, and acceptance tests 14 and
15 fail.

## Step 6: first run

```sh
gh workflow run coderabbit-maintainer-review.yml \
  --ref main -f pr_number=<n>
```

The run pauses at the environment gate for a maintainer approval, then posts one
`COMMENT` review carrying the head SHA, the trusted policy hash, and the CLI
version.

To repeat a review of a head SHA that was already reviewed:

```sh
gh workflow run coderabbit-maintainer-review.yml \
  --ref main -f pr_number=<n> -f force=true -f reason="<why>"
```

`force` without a reason is rejected, and the reason is published in the review.

---

## Acceptance test matrix

Run this before declaring the integration live, and again after any change to
the workflow, the trusted policy, the ruleset, or the environment.

Everything that can be decided without a GitHub account, a CodeRabbit
credential, or an administrator console is executable:

```sh
scripts/coderabbit-acceptance-tests.sh                    # offline, ~2 seconds
VERIFY_DOWNLOAD=1 scripts/coderabbit-acceptance-tests.sh  # also verify the pinned CLI digest
```

That suite runs the authorization gate's real shell, extracted from the
workflow file, against a mock GitHub, so it cannot drift from what ships. It
ends by listing the cases it could not decide, so a green run is never mistaken
for a complete one. It is deliberately not wired into `ci.yml`: adding a job
there would mean editing a workflow this change has no business touching.

`I` marks an implementation control that a test account can exercise directly.
`M` marks a control that lives in GitHub or CodeRabbit administration and needs
manual evidence. `nofire-bot` and the 13 `write` collaborators are real
identities available for the `write` cases.

| # | Case | How to test | Expected result | Type |
|---|---|---|---|---|
| 1 | `read` user cannot invoke | Sign in as a `read` user, attempt `gh workflow run coderabbit-maintainer-review.yml --ref main -f pr_number=<n>` | HTTP 403 from GitHub. Dispatch requires `write`, and even with it the `role_name` check rejects | I |
| 2 | `triage` user cannot invoke | Same, as a `triage` user | HTTP 403 | I |
| 3 | `write` user cannot invoke | Same, as a `write` user (`nofire-bot`) | Dispatch is accepted by GitHub, then the `authorize` job fails at "Verify actor holds maintain or admin" with `role 'write'`. No environment requested, no secret reachable, no CodeRabbit call | I |
| 4 | Organization member without `maintain` | Same, as an organization member holding `write` or less | As 1 to 3. Membership is never consulted | I |
| 5 | Outside collaborator cannot invoke | Same, as an outside collaborator with `write` | As 3 | I |
| 6 | External fork author cannot invoke via any surface | From a fork: open a PR; add labels; write `@coderabbitai review` in the description, in a comment, in a commit message; push again | No workflow run starts. No CodeRabbit call. The workflow has no `pull_request`, `pull_request_target`, `issue_comment`, `pull_request_review`, `workflow_run`, `repository_dispatch`, `schedule`, or `push` trigger. With the App removed or its org settings applied, the comment produces nothing | I + M |
| 7 | `maintain` user can review | Dispatch as a `maintain` holder against an open PR targeting `main` | `authorize` passes with `role_name=maintain`, environment approval requested, one `COMMENT` review published | I |
| 8 | `admin` can review | Same, as an `admin` | As 7, with `role_name=admin` | I |
| 9 | Wrong base branch rejected | Dispatch against a PR whose base is not `main` | "Validate the pull request" fails: `base branch is '<x>', expected main` | I |
| 10 | Closed PR rejected | Dispatch against a closed or merged PR | Fails: `pull request is 'closed', not open` | I |
| 11 | Stale head rejected | Dispatch, then push a new commit to the PR before approving the environment | The review job fails: `head moved from <a> to <b>` | I |
| 12 | Duplicate skipped | Dispatch twice for the same head, same policy, same CLI version | Second run: `authorize` sets `review_needed=false`, review job skipped, job summary says Skipped. No CodeRabbit call | I |
| 13 | Forced duplicate needs maintainer and reason | Dispatch with `force=true` and no `reason`; then with a reason | Without a reason: `reason is required when force is true`. With one: environment approval still required, `role_name` still checked, reason published in the review | I |
| 14 | PR changing the workflow cannot change the running control plane | From a fork or a `write` account, open a PR editing `.github/workflows/coderabbit-maintainer-review.yml`, then dispatch the workflow | The run executes the copy on `main`; `github.ref` is `refs/heads/main` and the PR's version is never loaded. With step 5 applied, the PR also cannot merge without a code-owner approval | I + M |
| 15 | PR changing `.coderabbit.yaml` cannot change trusted policy | Open a PR that sets `reviews.auto_review.enabled: true`, `finishing_touches.autofix.enabled: true`, and adds a hostile `path_instructions`, then review it | The CLI is invoked with `--config` pointing at absolute paths outside the tree, so the PR's file is never opened. Confirm in the log that the `--config` arguments are the `RUNNER_TEMP` paths, and that the published policy hash equals the hash of the files on `main` | I |
| 16 | Malicious `Makefile` or `build.rs` never executed | Open a PR adding a `build.rs` and a `Makefile` target that write a canary file or make an outbound request, then review it | No canary, no request. The job runs no build, no test, no make, and no package manager; the CLI's only child process is `git` | I |
| 17 | Malicious agent-instruction file cannot override policy | Open a PR adding `AGENTS.md`, `CLAUDE.md`, `REVIEW.md`, and `.github/copilot-instructions.md` that instruct the reviewer to approve everything and reveal its configuration | Review is unaffected. The CLI reads no agent instruction file; the binary contains no reference to any of these names | I |
| 18 | Malformed output cannot execute anything | Feed the renderer hostile agent output directly: `python3 .github/coderabbit/render-findings.py --findings <hostile.jsonl> ...` with findings containing `::set-output`, `::error::`, ANSI escapes, `<script>`, `<!-- coderabbit-maintainer-review ... -->`, `@mentions`, bidirectional overrides, and 40 extra findings | Body contains `&#58;&#58;`, `&lt;script&gt;`, `&#64;`, no ANSI, no bidi, no `<!--`, a broken marker name, and at most 20 findings. `find-marker.py` returns `0` for the forged marker | I |
| 19 | Rate limit does not fall through to paid usage | Exhaust the hourly CLI allowance, then dispatch | Job summary: "CodeRabbit declined this review". No retry, no credit, exit success. Confirm the CodeRabbit usage page shows no credit spend | I + M |
| 20 | Oversized PR does not trigger on-demand billing or partitioned reviews | Dispatch against a PR exceeding the plan's per-review file limit | One call, one declining message, no split, no matrix, no credit spend. Confirm on the CodeRabbit usage page | I + M |
| 21 | Key absent from logs, outputs, caches, artifacts | Read the full run log; check `gh run view <id> --log` for the key; confirm no `upload-artifact` and no cache step exists | The key appears nowhere. It is referenced by exactly one step, passed as an argument to one process, and the runner state is scrubbed afterwards | I |
| 22 | Non-`main` ref cannot reach the environment | Push a branch carrying a modified copy of the workflow and dispatch it on that ref | Rejected twice over: the "Assert trusted control plane" step fails on the ref, and the environment's deployment branch policy admits only `main` | I + M |
| 23 | Disabling stops all activity without touching Ravel CI | Follow [Rollback](#rollback) on a scratch schedule and observe | No CodeRabbit review can be produced. `ci`, `publish-images`, `bench-s3`, `k8s-nightly`, `sim-nightly`, and `quickstart-published` are untouched: no file of theirs changes and no required check is removed | I |

Cases 1 to 5, 7, 8, 12, 16, and 18, and the policy and cost-control assertions
behind 15, 17, 19, and 20, are covered by `scripts/coderabbit-acceptance-tests.sh`
and pass. The remaining cases need real accounts or an administrator console,
and are marked `M` above.

---

## Cost-control checklist

Run this at enablement, and again whenever the plan, the pin, or the workflow
changes.

- [ ] CodeRabbit organization shows **Open Source**, not Free, Pro, Pro+, or a trial
- [ ] `NOFireAI/ravel` is receiving OSS treatment
- [ ] Usage-based add-on **disabled**, and confirmed not enableable on this plan
- [ ] No payment method backing this organization, and no purchased credits
- [ ] No paid fallback configured anywhere in the workflow
- [ ] Review-on-demand billing not used
- [ ] Exactly one CodeRabbit invocation per run, with no retry in the workflow, the script, or CI
- [ ] Concurrency group keyed on repository and PR number, `cancel-in-progress: false`
- [ ] Deduplication marker checked before every call
- [ ] No matrix over crates, directories, or file groups
- [ ] Rate-limit and over-limit responses exit neutral and spend nothing
- [ ] `.coderabbit.yaml` and the trusted config both disable automatic and incremental review
- [ ] App automatic review disabled at organization level, or App removed from `ravel`
- [ ] CodeRabbit usage page reviewed monthly and showing zero credit spend

---

## Credential rotation and incident response

**Routine rotation, every 90 days and on any maintainer offboarding:**

1. Create a new Agentic API key in the CodeRabbit dashboard.
2. Update the `CODERABBIT_API_KEY` secret on the `coderabbit-oss` environment.
3. Delete the old key in the CodeRabbit dashboard. Deleting it there is what
   makes the old value useless; overwriting the GitHub secret alone does not.
4. Dispatch one review to confirm the new key works.

**If the key is suspected exposed:**

1. **Revoke first.** Delete the key in the CodeRabbit dashboard. Do not start
   with the GitHub secret: the secret is a copy, the key is the credential.
2. Delete the `CODERABBIT_API_KEY` secret from the environment. With no secret,
   the review job fails closed at its first step.
3. Check the CodeRabbit usage page for reviews the maintainer set did not
   dispatch. Cross-reference against
   `gh run list --workflow coderabbit-maintainer-review.yml --json databaseId,actor,createdAt,conclusion`.
4. Determine the exposure route. The plausible ones are: an environment
   protection rule that was relaxed; a change to the workflow that widened the
   secret's blast radius; a maintainer account compromise; a copy of the key
   outside GitHub. Check
   `gh api repos/NOFireAI/ravel/environments/coderabbit-oss --jq '.protection_rules'`
   and `git log --oneline -- .github/workflows/coderabbit-maintainer-review.yml .github/coderabbit/`.
5. Issue a new key only after the route is closed.
6. If a review was produced that no maintainer dispatched, treat the repository's
   review history as untrusted for that window and re-verify anything merged on
   the strength of it.

**If CodeRabbit itself is compromised, or its output turns hostile:** the blast
radius is bounded by design. The integration has `contents: read` and
`pull-requests: write`, publishes only `COMMENT` reviews, cannot push, cannot
merge, cannot open issues, and cannot approve or block. Follow
[Rollback](#rollback) and open an issue.

---

## Rollback

Ordered from fastest to most complete. Each step is independently sufficient for
what it claims, and none of them touches Ravel's other workflows.

1. **Stop all spending immediately.** Delete the `CODERABBIT_API_KEY` secret
   from the `coderabbit-oss` environment. The review job then fails at its first
   step and no CodeRabbit call can be made.
2. **Stop all dispatches.** Disable the workflow:
   `gh workflow disable coderabbit-maintainer-review.yml`.
3. **Stop the App as well.** Remove `NOFireAI/ravel` from the CodeRabbit App
   installation in GitHub organization settings. This is what stops
   `@coderabbitai` comment surfaces; disabling the workflow does not.
4. **Remove the integration from the repository.** Delete
   `.github/workflows/coderabbit-maintainer-review.yml`, `.github/coderabbit/`,
   `.coderabbit.yaml`, `scripts/coderabbit-maintainer-review.sh`, and the
   CodeRabbit entries in `.github/CODEOWNERS`, in one pull request. Mark
   ADR-0091 superseded rather than deleting it.
5. **Remove the environment.**
   `gh api --method DELETE repos/NOFireAI/ravel/environments/coderabbit-oss`.

None of these removes a required status check, so `main` stays mergeable
throughout. Ravel's CI is unaffected: no file under `.github/workflows/` other
than the CodeRabbit workflow is touched by any step, and CodeRabbit is not in
the `protect-main` required-checks list.

---

## Local maintainer fallback

Use this when step 1 fails, or before the environment exists.

Install the pinned CLI. Do not use `curl | sh`: it is unpinned and unverified.

```sh
version=$(grep '^CODERABBIT_CLI_VERSION=' .github/coderabbit/cli-pin.env | cut -d= -f2)
expected=$(grep '^CODERABBIT_CLI_SHA256_DARWIN_ARM64=' .github/coderabbit/cli-pin.env | cut -d= -f2)
curl --fail --location --proto '=https' --tlsv1.2 \
  -o coderabbit.zip \
  "https://cli.coderabbit.ai/releases/${version}/coderabbit-darwin-arm64.zip"
echo "${expected}  coderabbit.zip" | shasum -a 256 -c -
unzip -o coderabbit.zip -d ~/.local/bin
```

Use `coderabbit-linux-x64` or `coderabbit-linux-arm64` and the matching digest
on Linux. Then `coderabbit auth login` with your own account, and:

```sh
scripts/coderabbit-maintainer-review.sh 1234              # print locally
scripts/coderabbit-maintainer-review.sh 1234 --publish    # post a COMMENT review
```

The script refuses to run unless GitHub reports your `role_name` as `maintain`
or `admin`. It holds no shared credential, so there is nothing here for a
non-maintainer to consume: the allowance spent is your own.

## Upgrading the pinned CLI

ADR-0091's Decision 4 depends on one property of the pinned build: a non-empty
`--config` list replaces the CLI's discovery list rather than adding to it, and
an absolute path is used verbatim. A version bump can change that silently,
because the vendor's own help text describes the flag as additive.

Before changing `.github/coderabbit/cli-pin.env`:

1. Download the new versioned artifact and record its SHA-256.
2. Confirm the replacement behaviour still holds in the new build. In 0.7.3 the
   deciding expression is
   `h = $.configFiles && $.configFiles.length > 0 ? $.configFiles : [".coderabbit.yaml", ...]`
   and the resolver is `if (isAbsolute($)) return [$]`.
3. Confirm the agent output shape has not changed, since
   `render-findings.py` parses `{"type":"finding", ...}`.
4. Confirm the CLI still executes nothing from the reviewed tree: no linter
   binaries, no linter names, `git` as its only child process.
5. Update the pin and the digests in one pull request, with a code-owner
   approval, and record what you checked.

If any of 2 to 4 fails, do not upgrade. Say so in the pull request.

---

## A note on the diff base

The workflow reviews `merge-base(base.sha, head.sha)` to `head.sha`, where
`base.sha` is what GitHub reported when the pull request was validated. Both
inputs are immutable for the run, so the diff base is reproducible and does not
move when `main` does, which is what the "capture once, treat as immutable"
requirement asks for.

One consequence worth knowing: for a pull request that sat stale and was then
rebased, that merge base can be older than the branch point, so the review sees
some commits from `main` as though the pull request wrote them. Ask the author
to rebase and dispatch again if a review reads that way.

## Residual risks

These are real and are not closed by anything in this repository.

**The CLI has no vendor integrity artifact.** No checksum, no signature, no
image digest. The pinned digests are trust-on-first-use: they attest that CI
runs the same bytes captured on 2026-08-19, not that those bytes are what
CodeRabbit intended to publish. A compromise of the vendor's distribution before
that capture is undetectable from here. Mitigating this properly needs the
vendor to publish signed artifacts.

**Three plan facts are unverified from this session.** The CodeRabbit
organization's plan, the usage-based add-on state, and Agentic API key
availability all live behind a dashboard login. Step 1 is where they get
established, and the integration must not be enabled before it passes.

**The App's boundary is organization membership, not maintainership.** If the
App stays installed for OSS entitlement, and if any non-maintainer identity can
still trigger it after step 2, then a `write` collaborator can consume the
allowance through the App even though the workflow denies them. This is why step
2 requires the test from real accounts rather than a settings screenshot.

**The API key is passed as a process argument.** The CLI reads no environment
variable for it, so `--api-key` is the only option. On a GitHub-hosted ephemeral
runner the process list is visible only to the same job, which runs nothing from
the pull request, and the runner is destroyed afterwards. The exposure is small
but it is not zero, and it would grow if any step in that job ever ran untrusted
code.

**The CLI carries product telemetry.** It bundles an analytics client with
exception auto-capture. Diagnostics about a review, including error text, may
leave the runner. No Ravel secret is in scope, but this is not a hermetic tool.

**GitHub's `workflow_dispatch` permission is `write`.** A `write` user can
always start a run. They are stopped by the `role_name` check and by the
environment, and a rejected run costs a few seconds of a hosted runner. It is
not free, and a determined `write` user could dispatch repeatedly. If that ever
happens, revoke their access; there is no workflow-level fix, because GitHub
does not expose the trigger permission this design would want.

**Ruleset and environment settings are not code.** Nothing in this repository
can assert that `require_code_owner_review` is true or that the environment
admits only `main`. The verification commands in steps 3 and 5 are the only
check, and they are only as good as the cadence at which someone runs them.

**Maintainer set drift.** CODEOWNERS lists logins because there is no maintainer
team on this repository yet. Reconcile it against the live collaborator list
whenever roles change, and prefer a `@NOFireAI/ravel-maintainers` team once one
exists.

**A review is one model's opinion of one revision.** It is not an approval, not a
gate, and not evidence. Ravel's gates are `scripts/gates.sh` and the required
checks in `protect-main`, and human review remains the merge decision.

# ADR-0091: maintainer-gated CodeRabbit reviews

Status: Proposed

Documentation, plan behaviour, and CodeRabbit CLI behaviour in this ADR were
verified on 2026-08-19. Everything below that is stated as a fact was checked
against the vendor's own documentation, against the GitHub API for this
repository, or against the shipped CLI binary. Everything that could not be
checked is named as unresolved, and the design fails closed on each one.

## Context

Ravel is a public repository. It has 20 collaborators: 7 hold `admin` and 13
hold `write`. That distribution is the whole problem. Almost every control an
AI review vendor offers is drawn at the boundary "organization member or not",
or at "can comment on a pull request", and for `NOFireAI/ravel` both of those
boundaries sit far below `maintain`.

The CodeRabbit GitHub App is already installed on this organization with
repository selection `selected`, and it is already reviewing Ravel pull requests
(PRs #292 and #294 carry `coderabbitai[bot]` reviews). There is no
`.coderabbit.yaml` in the repository, so the App is running on its defaults:
automatic review on, automatic incremental review on, autofix on, fix-CI on,
unit-test generation on, docstring generation on, merge-conflict resolution on,
chat auto-reply on, and `chat.allow_non_org_members` on. Every one of those is a
surface a non-maintainer can reach today, and several of them write to the
repository.

The property this ADR has to establish is narrow and absolute:

> A user who is not a maintainer of `NOFireAI/ravel`, where maintainer means
> effective repository permission `maintain` or `admin`, must not be able to
> cause a CodeRabbit API call, consume this repository's CodeRabbit allowance,
> reach a CodeRabbit credential, change the trusted CodeRabbit policy, or make
> CodeRabbit modify repository content.

Users with `read`, `triage`, or `write`, organization members without
`maintain`, outside collaborators, pull-request authors, CODEOWNERS without
`maintain`, and anyone whose author association is MEMBER, COLLABORATOR, or
CONTRIBUTOR are all non-maintainers for this purpose.

### What the vendor's plans actually provide

CodeRabbit's plans page states, for Open Source: "Unlimited public
repositories, no credit card required", with OSS projects receiving Pro+
features and rate limits that vary by project popularity. The published
per-developer, per-hour rate limits give OSS 1 to 10 pull-request reviews, 1 IDE
review, 3 CLI reviews, 100 to 300 files per review, and 25 chat messages.

Two facts about billing matter more than the limits themselves.

The usage-based add-on, which converts an over-limit review into on-demand
credits at a per-file price, is documented as available to Pro, Pro+, and
Enterprise. It is not offered on the Open Source plan. An organization that is
genuinely on the Open Source plan therefore has no mechanism by which a review
becomes chargeable: the plan's response to exhaustion is a rate-limit message,
not a bill.

The headless CLI path, which is the only way to run CodeRabbit from CI without a
browser, is documented as follows: "The non-interactive API-key flow requires an
Agentic API key from a CodeRabbit organization where the user has an assigned
seat", "API-key authentication always uses that key's organization for billing
and review behavior", and "CLI reviews use the assigned user's plan allowance
first". Whether an Open-Source-plan organization has an assignable seat capable
of minting an Agentic API key is not stated anywhere in the public
documentation, and could not be verified from this session, which has no access
to the CodeRabbit dashboard.

That single unresolved question is what splits this design into a primary path
and a fallback, rather than collapsing it into one.

### What the CodeRabbit CLI actually does

The CLI was pinned, downloaded, and read, at version 0.7.3, rather than trusted
from its help text. Four findings shaped the design.

First, and decisively: `-c, --config <files...>` is documented as "Additional
instructions for CodeRabbit AI (e.g., claude.md, coderabbit.yaml)", which reads
as additive and would make the whole approach unsafe. The shipped code says
otherwise. The configuration list is

```js
h = $.configFiles && $.configFiles.length > 0
      ? $.configFiles
      : [".coderabbit.yaml", ".coderabbit.yml", "coderabbit.yaml", "coderabbit.yml"]
```

so a non-empty `--config` **replaces** the discovery list. It does not extend
it. The path resolver is

```js
function MT$($, v, I) { if (Ko.isAbsolute($)) return [$]; ... }
```

so an absolute path is used verbatim, with no resolution against the repository
being reviewed. Passing the trusted policy by absolute path from outside the
worktree therefore means the pull request's own `.coderabbit.yaml` is never
opened. This is a property of this build, which is why the build is pinned.

Second, the CLI does not ingest agent instruction files. The binary contains no
occurrence of `AGENTS.md`, `AGENT.md`, `.cursorrules`, `.windsurfrules`,
`GEMINI.md`, or `copilot-instructions`. The two `CLAUDE.md` strings in it belong
to the Bun runtime's own project scaffolding, not to review logic.

Third, the CLI runs no analysis locally. It has no linter binaries and no
linter names in it, and its only child process is `git`, through `simple-git`.
The review payload it builds is `{filename, diff, fileContent}` per changed
file, sent to the service. So pointing the CLI at a hostile tree does not
execute the tree, and a `Makefile`, a `build.rs`, an npm lifecycle hook, or a
linter configuration in the pull request is inert.

Fourth, agent output is JSONL with exactly one shape,
`{"type":"finding","severity","fileName","codegenInstructions","suggestions"}`
plus `comment`, and severity is one of `critical`, `major`, `minor`, `info`.
There are no line numbers in it. A review that claims line anchors would be
inventing them.

Two more facts about the distribution: the vendor publishes no checksum file, no
signature, and no container image for the CLI. Every plausible integrity
artifact under `releases/<version>/` returns 404, and no image exists on GHCR or
Docker Hub. The documented installation is `curl -fsSL
https://cli.coderabbit.ai/install.sh | sh`, and reading that script confirms it
performs no verification of what it downloads. It does honour
`CODERABBIT_VERSION`, and versioned artifacts are served from immutable-looking
paths, so a version can be pinned even though its authenticity cannot be
established from the vendor.

### What GitHub actually provides

GitHub has no setting that restricts `workflow_dispatch` to `maintain` and
above: the ability to dispatch a workflow comes with `write`. The authorization
boundary this ADR needs does not exist as a repository setting, so it has to be
built.

Two mechanisms were tested rather than assumed.

The default `GITHUB_TOKEN`, with `permissions: contents: read` and nothing else,
can read `GET /repos/{owner}/{repo}/collaborators/{actor}/permission`. This was
verified by running it in Actions on this repository on 2026-08-19: exit 0,
`role_name=admin`. So the least-privileged option in the task's fallback ladder
is available, and no GitHub App credential and no classic PAT are needed.

That response has a trap. Its legacy `permission` field collapses `maintain`
into `write`. Only `role_name` distinguishes them. Ravel has zero `maintain`
users today, so a test run by an admin would pass either way and the bug would
ship invisible. The implementation reads `role_name` and matches it exactly
against `admin` and `maintain`.

The repository's `protect-main` ruleset today requires linear history, blocks
deletion and force-push, and requires 15 status checks, but sets
`required_approving_review_count: 0` and `require_code_owner_review: false`.
Under that ruleset a `write` user can open a pull request that edits the
CodeRabbit workflow or its policy files and merge it themselves once CI is
green. That is "a non-maintainer changes the trusted CodeRabbit policy", and it
is a hole in the property this ADR is establishing, independent of anything
CodeRabbit does.

## Decision 1: the CodeRabbit GitHub App is not an authorization mechanism

Rejected as the primary control, on evidence.

CodeRabbit's own documentation states that "the configuration present in the
feature branch under review will be automatically detected and used by
CodeRabbit for that review". Configuration for the App is read from the head
branch. Every pull request, including one from a fork, therefore ships its own
policy. A repository-owned `.coderabbit.yaml` on `main` governs nothing about
the review of a pull request that changes it.

The chat surface is documented as "You can mention CodeRabbit using
`@coderabbitai` in any pull request comment to start a conversation", with no
documented permission requirement. The finest-grained restriction the schema
offers is `chat.allow_non_org_members`, an organization-member boundary. For
`NOFireAI/ravel` that boundary admits 13 `write` collaborators who are not
maintainers, so even at its most restrictive setting it is not the boundary this
ADR requires. Stating otherwise would be false.

Therefore: the App must not be the mechanism by which anyone obtains a review,
and this design does not describe it as secure. Its automatic and write-capable
surfaces are turned off in CodeRabbit's organization settings, which is the only
layer a pull request cannot edit, and the repository keeps a `.coderabbit.yaml`
that closes the same surfaces as defence in depth against an accidental
regression, never as a control. The preferred posture is to remove
`NOFireAI/ravel` from the App's installation entirely; the runbook keeps that
decision open pending one unresolved question, namely whether Open Source
entitlement requires the installation. The vendor's pricing page says "install
CodeRabbit on a public repository, and receive free reviews forever for public
repositories", which reads as requiring it, and no documentation contradicts
that.

## Decision 2: reviews come from a manually dispatched, maintainer-gated workflow

`.github/workflows/coderabbit-maintainer-review.yml` is the only supported way
to obtain a CodeRabbit review of a Ravel pull request. It triggers on
`workflow_dispatch` and, per the amendment below, on `issue_comment`. It carries
none of `pull_request`, `pull_request_target`, `pull_request_review`,
`workflow_run`, `repository_dispatch`, `schedule`, or `push`, because each of
those starts a review with no human intent behind it.

It is layered, and the layers are independent:

1. **Trusted control plane.** The run aborts unless the repository is
   `NOFireAI/ravel`, the event is `workflow_dispatch`, and the ref is
   `refs/heads/main`. Re-asserted in the second job, after the environment
   approval, because the two jobs are separated in time.
2. **Runtime permission verification.** `role_name` for `github.actor`, matched
   exactly against `admin` and `maintain`. A missing, malformed, or failed
   response fails the run. No author association, no organization membership, no
   team, no CODEOWNERS entry, and no workflow input participates.
3. **Protected environment.** The credential is an environment secret on
   `coderabbit-oss`, with maintainer reviewers required, self-review prevented,
   and a deployment branch policy admitting only `main`. It is never a
   repository secret, never an organization secret, never a variable, never a
   file, and never an output or artifact.
4. **Job separation.** The authorizing job holds no environment and no secret.
   A denied actor never reaches the environment gate, so a rejection is never
   also an approval request in a maintainer's inbox.

The two jobs mean the permission check is not a formality that runs alongside
the secret; it runs before it exists.

## Decision 3: the pull request is data, never code

The secret-bearing job fetches the pull request into `RUNNER_TEMP`, through
GitHub's read-only `refs/pull/<n>/head` ref in the base repository, with a
remote URL that is this workflow's own constant. No contributor-controlled
string, not a fork URL, a branch name, a title, or a login, is ever interpolated
into a shell command. Git hooks are disabled and object fsck is enabled on the
fetch.

Nothing in that tree is built, tested, linted, installed, sourced, executed, or
cached. `scripts/gates.sh`, `cargo`, the `Makefile`, and every package-manager
lifecycle hook stay unrun. The runner is GitHub-hosted and version-pinned
(`ubuntu-24.04`), never self-hosted. No artifact is uploaded and no cache is
restored. The job's permissions are `contents: read` and `pull-requests: write`,
with everything else denied by an empty top-level `permissions: {}`.

This is safe specifically because of the third CLI finding above. If the CLI ran
local analyzers, a contributor-supplied linter configuration would be code
execution next to a live credential, and this decision would not hold.

## Decision 4: policy is loaded from main, by absolute path, from outside the tree

`.github/coderabbit/trusted-config.yaml` and
`.github/coderabbit/ravel-review-instructions.md` are checked out from `main`,
copied to a directory outside every pull-request tree, and passed to the CLI as
absolute paths. By Decision 1's evidence about `--config`, the CLI then never
opens the pull request's `.coderabbit.yaml`, and by the second CLI finding it
never opens `CLAUDE.md`, `AGENTS.md`, or any other agent instruction file.

Staging outside the worktree is not cosmetic. The CLI merges a `--config` file
into the review payload keyed by filename, and overwrites a changed file's
content when the names collide. A trusted file at a repository-relative path
that the pull request also changed would silently replace that file's content in
the payload, and the reviewed diff would no longer be the real diff. An absolute
path outside the tree cannot collide.

The trusted configuration disables every capability that can write to the
repository or generate unbounded activity: autofix, fix-CI, unit-test
generation, docstring generation, merge-conflict resolution, custom finishing
touches, pre-merge checks, post-merge actions, automatic review, automatic
incremental review, chat and chat auto-reply, non-organization-member chat,
issue creation, issue planning, issue enrichment, automatic labelling, Jira and
Linear integrations, web search, MCP, knowledge-base learnings, and repository
linking. It also disables every static tool that reads a configuration file out
of the reviewed tree, since such a file is contributor-controlled input to a
tool. `gitleaks`, `trufflehog`, `actionlint`, and `zizmor` stay on: none of them
reads configuration from the tree, and a leaked credential or a broken workflow
in a diff is worth the allowance. `clippy` is off because Ravel already runs it
as a required check with `-D warnings`, and a second opinion would spend the
file budget on findings Ravel already treats as blocking.

## Decision 5: one review per exact head SHA, and no paid fallback anywhere

The workflow validates against GitHub, once, that the pull request exists, is
open, targets `main` in `NOFireAI/ravel`, has a non-empty diff, and has a
40-character head SHA. Those values are then immutable for the run. Before
CodeRabbit is called, the head SHA is re-read and the run aborts if it moved
during the environment approval. The diff base is `merge-base(base_sha,
head_sha)`, derived from two immutable inputs so that it does not move when
`main` does.

Deduplication is a marker in the review body, carrying the head SHA, the hash of
the trusted policy, and the CLI version. Before calling CodeRabbit, the workflow
searches reviews authored by `github-actions[bot]`, matching the marker
structurally inside a single body and only inside a real HTML comment. A
repeated review of the same triple requires all four of: `force`, a still-valid
maintainer role, an approved environment deployment, and a non-empty audit
reason that is recorded in the published review.

Concurrency is keyed on repository and pull-request number, without
cancel-in-progress: cancelling a run that has already called CodeRabbit would
spend the allowance and publish nothing.

There is exactly one CodeRabbit invocation per run. No retry. No second attempt.
No splitting of a large pull request into several calls to fit the plan's
per-review file limit, and no matrix over crates, directories, or file groups. A
rate-limit or over-limit response produces a bounded status message in the job
summary and exits successfully, having spent nothing. The absence of a paid
fallback is the design, not an omission: on the Open Source plan no credit
mechanism exists, and this integration must not be the reason one gets enabled.

## Decision 6: the CLI is pinned by version and by a digest this repository owns

`.github/coderabbit/cli-pin.env` pins version 0.7.3 and records SHA-256 digests
for the linux-x64, linux-arm64, and darwin-arm64 artifacts. CI downloads the
versioned artifact directly, verifies the digest, and refuses to run on a
mismatch. No installer script runs, and `CODERABBIT_CLI_DISABLE_AUTO_UPDATE=1`
prevents the binary from replacing itself.

The digests are trust-on-first-use, and this is a real supply-chain gap, stated
plainly rather than papered over: the vendor publishes nothing to verify against,
so the first capture is trusted on faith and everything after it is verified
against that capture. The alternative on offer was an unpinned `curl | sh`,
which is strictly worse and would have made "the design is secure" a false
claim.

The pin also freezes audited behaviour. Decision 4 depends on how this exact
build resolves `--config`. Raising the version requires re-verifying that
property before the digests change.

Every GitHub Action is pinned to a full commit SHA with the release version in a
trailing comment, matching the convention already used across Ravel's workflows.
No third-party action is added; the workflow uses `actions/checkout` and nothing
else.

## Decision 7: CodeRabbit's output is untrusted data

The model's output selects nothing. Not the repository, not the pull request,
not the API endpoint, not the review state, not a workflow command, not a shell
command. Those are all fixed in the workflow or read from the GitHub API.

`.github/coderabbit/render-findings.py`, loaded from `main`, parses the agent
JSONL strictly and bounds it in every dimension: 2 MiB of input, 20000 lines, 20
findings, 1200 characters per finding, 3 suggestions of 1500 characters each,
and a 60000-character review body against GitHub's 65536 limit. It strips ANSI
escapes, C0 and C1 control characters, bidirectional overrides, and zero-width
characters; neutralises `@` to `&#64;` so nothing can notify a person; escapes
`<` and `>` in prose; removes HTML comment delimiters; breaks the deduplication
marker's own name so a finding cannot forge one; neutralises a leading `::` so a
line can never read as a workflow command; and breaks code fences inside
suggestions. Duplicates are collapsed, `info` findings are dropped as the
severity where style and praise land, and the remainder is sorted by severity.

Exactly one review is published per successful run, always with event `COMMENT`,
never `APPROVE` and never `REQUEST_CHANGES`, carrying the reviewed head SHA, the
trusted policy hash, the CLI version, and the dispatching actor and role. The
request body is passed to `gh api` as a file, so no part of the model's output
ever becomes a shell word. Because agent output has no line numbers, findings
are grouped by file rather than anchored to lines. That is the designed shape,
not a degraded fallback.

## Decision 8: a local maintainer fallback ships regardless

`scripts/coderabbit-maintainer-review.sh` reviews one pull request with the
maintainer's own CodeRabbit login and their own `gh` authentication. It verifies
`role_name` the same way, validates the pull request the same way, fetches it as
inert data the same way, loads policy from `origin/main` the same way, requires
the pinned CLI version, prints findings locally, and posts a review only with an
explicit `--publish`.

It exists for two reasons. If the Open Source plan turns out not to include an
Agentic API key, this is how Ravel gets CodeRabbit reviews without buying a
seat. And because it holds no shared credential and no shared automation
identity, there is nothing for a non-maintainer to consume: the allowance spent
is the individual maintainer's own.

## Decision 9: the trusted paths get code owners, and the ruleset has to change

CODEOWNERS entries covering the workflow, the policy directory,
`.coderabbit.yaml`, the fallback script, and CODEOWNERS itself are added, owned
by the repository's admins.

CODEOWNERS alone requests a review; it does not require one. Until
`protect-main` sets `require_code_owner_review: true` with a non-zero approval
count, a `write` user can still merge a change to the trusted control plane
themselves. That ruleset change is an administrator action, recorded in the
runbook, and it is a precondition of this ADR's security property rather than a
nicety. Until it is applied, the property does not hold, and the runbook says
so in those words.

## Decision 10: the acceptance matrix is executable where it can be

`scripts/coderabbit-acceptance-tests.sh` decides every acceptance case that does
not need a GitHub account, a CodeRabbit credential, or an administrator console:
the absent triggers, least privilege, action pinning, the digest pin, secret
exposure, the authorization gate, output sanitisation, marker forgery, the
policy's disabled capabilities, and the cost controls.

The authorization test does not reimplement the gate. It extracts the shipped
`run:` block from the workflow file and executes it against a mock GitHub, so
the test cannot drift from the control it is testing. That is what catches the
`role_name` trap: a `maintain` fixture whose legacy `permission` field says
`write` must pass, and a `write` fixture must not, and `NOFireAI/ravel` has no
real `maintain` user to notice with.

The suite ends by printing the cases it could not decide, with a pointer to the
runbook's manual evidence steps, so a green run is never read as a complete one.
It is not wired into `ci.yml`, because this change has no business editing a
workflow that is not its own.

## Amendment: a maintainer may also start a review by comment

The original decision allowed only `workflow_dispatch`. Dispatching from the
Actions tab or the `gh` CLI is a context switch away from the pull request being
reviewed, which is where the decision to review it is actually made. The
workflow now also triggers on `issue_comment`, and a maintainer starts a review
by commenting on the pull request:

```text
/coderabbit review
/coderabbit review force: <audit reason>
```

**The comment is not the authorization.** It is a request. Authorization is the
same `role_name` check as before, and the credential sits behind the same
protected environment. What the comment changes is who can cause a workflow
*run*, not who can cause a CodeRabbit *call*.

Three properties make this safe to add.

`issue_comment` always runs the copy of the workflow on the default branch, with
`GITHUB_REF` set to the default branch. So a pull request cannot alter the
control plane by commenting on itself, the existing `refs/heads/main` assertion
still holds, and the environment's main-only deployment branch policy still
admits the run. The assertion is kept rather than relaxed: if GitHub ever
changed that, the feature would stop working instead of starting to trust a
pull request.

The command grammar is fixed and matched against the first line only. A command
quoted further down a comment is prose, so one person's paste cannot act on
another person's behalf. The comment body never becomes part of a command: it
arrives as an environment variable, is matched against a literal, and the only
value extracted from it is the audit reason, which is stripped of every control
character before it crosses a step boundary. Stripping newlines specifically
matters: without it, a reason could forge additional `key=value` lines in
`GITHUB_OUTPUT`.

Nothing answers a non-maintainer. An authorised command gets a reaction on the
comment, from a small job holding `issues: write` and nothing else, which runs
only after authorization succeeded. An unauthorised one gets silence and a
failed run that only people with repository access can see. Replying would turn
this into an amplifier for anyone who can type.

The cost, stated plainly: anyone who can comment, which on a public repository
is anyone, can now cause a workflow run to start and fail. A pre-filter on the
job (the comment is on a pull request, the author is not a bot, the body starts
with the command, and the author association is at least COLLABORATOR) keeps
ordinary drive-by comments off the runner entirely. That filter is an extra
condition, never an alternative to the permission check: MEMBER and COLLABORATOR
are both far wider than maintainer, and `role_name` is what decides. Runner
minutes on public repositories are free, so the residual exposure is noise in
the Actions tab, and a determined abuser is answered by revoking their access.

The trigger is a slash command rather than a mention on purpose.
`@coderabbit` is a real and unrelated GitHub user who would be notified on every
invocation, and `@coderabbitai` is the vendor App's handle.

## Rejected alternatives

**Native App reviews as the authorization mechanism.** Rejected on the evidence
in Decision 1: head-branch configuration, an open comment surface, and an
organization-member boundary that admits 13 non-maintainers here.

**`reviews.auto_review.enabled: false` in a repository `.coderabbit.yaml` as the
control.** Rejected: the file that governs a pull request's review is the copy
in that pull request.

**A label, a secret label name, or a magic phrase in the description.** Rejected:
`triage` and `write` users can apply labels, and any author writes the
description. None of these is an authorization decision.

**Author association, organization membership, or CODEOWNERS membership.**
Rejected: MEMBER, COLLABORATOR, and CONTRIBUTOR are explicitly not maintainers,
and a CODEOWNER need not hold `maintain`.

**An `@coderabbitai` comment command as the trigger.** Rejected: it is the
vendor App's own handle, so the App and this workflow would both act on one
comment, and the App's own handling of it is not maintainer-gated. See the
amendment for the trigger that was adopted instead.

**The legacy `permission` field from the collaborator-permission endpoint.**
Rejected: it reports a `maintain` user as `write`, so it cannot express this
boundary at all.

**A classic personal access token, or a GitHub App credential, for the
permission lookup.** Rejected as unnecessary. The default `GITHUB_TOKEN` was
tested and can perform the lookup, so introducing a broader credential would add
risk and buy nothing.

**An admin-maintained allowlist of maintainer logins.** Rejected as the primary
control: it drifts against the real roles, and a live API lookup does not. It
remains available as an additional control if GitHub ever changes the endpoint's
permissions.

**Enabling the usage-based add-on so large pull requests always complete.**
Rejected outright. It is the mechanism by which this integration would start
costing money, and the requirement is zero cost.

**Splitting an over-limit pull request into several CodeRabbit calls.** Rejected:
it is quota evasion, it multiplies the spend, and on a paid plan it multiplies
the bill.

**`curl | sh` to install the CLI.** Rejected: unpinned, unverified, and would
make any claim of supply-chain integrity untrue.

## Consequences

Reviews stop being automatic. A maintainer dispatches the workflow, approves the
environment, and gets one review of one revision. That is slower than the App,
and it is the cost of the property.

The workflow cannot spend anything until an administrator creates the
`coderabbit-oss` environment, protects it, and adds its secret. It is worth
being precise about which of those is load-bearing, because an earlier draft of
this ADR got it wrong: GitHub creates an environment the first time a workflow
names one, with no protection rules and no secrets. So a dispatch before setup
does not fail at a missing environment, it silently creates an unprotected one,
and the run then fails on the missing key. The failure mode that leaves behind
is an administrator adding the key to that auto-created environment and getting
no reviewer gate and no branch restriction, with everything looking configured.

The `authorize` job therefore verifies the environment's protection rules in
band, on every run, and fails closed: required reviewers present, self-review
prevented, a custom deployment branch policy naming exactly `main` and nothing
else. Setup is asserted rather than assumed. Verified on 2026-08-19 that the
default `GITHUB_TOKEN` with `contents: read` can read both the environments and
the deployment-branch-policies endpoints on this repository.

Three questions cannot be answered from a terminal and are named as runbook
preconditions rather than assumed: whether the CodeRabbit organization backing
`NOFireAI` is on the Open Source plan or a paid plan, whether the usage-based
add-on is disabled, and whether an Agentic API key can be issued for this
organization without a charge. This organization has CodeRabbit installed on
private repositories too, so a paid subscription is plausible, and a paid
subscription with the add-on enabled would make an over-limit CLI review
chargeable. If any of the three cannot be established, Decision 8's local
fallback is the whole integration and no environment is created at all.

Findings arrive grouped by file rather than on lines, because the CLI's agent
output has no line numbers. Reviewers open the file at the reviewed SHA.

The security property depends on two administrator actions that no file in this
repository can enforce: the environment's protection rules, and the
`require_code_owner_review` change to `protect-main`. Both are in the runbook,
with the evidence to check them and the named owner who can change them.

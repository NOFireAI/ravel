# ADR-1586: the fleet review bot is Ravel's agent review path

Status: Accepted (2026-09-10). Issue: #1586. Replaces ADR-0091, which is
deleted rather than marked superseded, along with the integration it
governed.

## Context

Ravel had a third-party AI review integration, and ADR-0091 existed because
of one property of it: the vendor read its review configuration from the pull
request's own head branch, and a review call spent a credential. Any pull
request could therefore ship its own review policy, and anyone who could
comment could spend the allowance. ADR-0091's answer was an authorization
boundary in this repository: one workflow, started only by hand or by a
maintainer's comment, verifying the actor's effective repository permission
was `maintain` or `admin` before the credential was reachable, loading policy
from `main` by absolute path, never executing pull-request code, and with
`pull_request`, `pull_request_target`, `workflow_run`, `repository_dispatch`,
`schedule` and `push` all deliberately absent as triggers so no review could
start without human intent.

That vendor is retired (owner directive, 2026-09-10) and its GitHub App is
uninstalled from the `NOFireAI` organization, so the workflow, its policy
files, its runner scripts and its runbook drove nothing while still
describing themselves as the repository's review gate. Reviews now come from
the fleet review bot, which is a GitHub App on this organization
(`app_slug: claude-fleet`, permissions `contents: read`, `issues: write`,
`metadata: read`, `pull_requests: write`) backed by the same executors and
model credentials as any other fleet task.

The bot is not a workflow and nothing in this repository responds to its
trigger. That is the point: it needs no repository-side control plane, and
the whole of ADR-0091's machinery goes away with it. What does not go away is
the question ADR-0091 was written to answer, which is who may cause a review
to spend a credential.

### What the bot does

A comment whose whole body is `@claude-fleet review` starts a review;
`@claude-fleet review <model> <effort>` names a model and one of `low`,
`medium`, `high`, `xhigh`, `max`. The words after `review` are parsed as
arguments, so a mention with prose after it gets a confused reaction and no
review at all.

Three replies follow, in order: an `eyes` reaction within seconds, meaning
the request passed its checks; a task comment carrying the task id, the
commit under review, the model and the executor, edited in place as the task
progresses; and, minutes later, the review, as a summary plus one inline
comment per finding on a changed line, with the rest listed in the body under
`Findings outside the diff:`.

The review is always a comment review. The bot never approves and never
requests changes, so no branch protection rule can be satisfied by it and
`COMMENTED` is its success state.

### How its authorization compares

Two of ADR-0091's three properties hold for the bot, and hold more simply.

Policy does not come from the pull request. The bot reads its review skill
from one fixed repository, with a separate installation token, and a missing
skill client fails every job at the skill fetch rather than falling back to
the triggering repository's token (`NOFireAI/claude-fleet`,
`internal/reviewbot/job.go:31-80`).

Pull-request content is data, not instructions. Title, description, changed
files and existing review comments enter the task under a line that marks the
section untrusted, inside a fence built so the content cannot close it early
(`internal/reviewbot/spec.go:38,75`). The agent also cannot push anywhere but
its own task branch, so a review cannot change the branch under review.

The third property is weaker than ADR-0091's. The bot gates on the comment's
GitHub `author_association`, admitting `OWNER`, `MEMBER` and `COLLABORATOR`
(`internal/reviewbot/webhook.go:61`), behind three further checks: the request
must come from an address GitHub publishes for webhooks, its signature must
match the App's webhook secret, and the repository must be on the
deployment's allowlist. `ravel` is public and has 23 collaborators, 16 of them
at `write` and 7 at `admin` (counted on 2026-09-10, not carried over from
ADR-0091's three-week-old figure), and `write` is a `COLLABORATOR`
association. So the boundary has moved from `maintain` to `write`, and it is
enforced in the bot rather than here.

## Decision

1. **`@claude-fleet review` is the review path.** The developer triggers it,
   or the skill that opened the pull request does;
   `scripts/fleet-result-merge.sh` posts it as its own comment after opening
   a PR. The trigger comment carries nothing but the trigger.

2. **Delete the previous integration outright, ADR-0091 included.** No
   configuration, workflow, policy file, runner script, runbook or CODEOWNERS
   entry for it remains. Every path `.github/CODEOWNERS` protected belonged
   to that control plane, and `protect-main` has
   `require_code_owner_review: false`, so the file requested reviews rather
   than requiring them; it is deleted with the rest. A maintainer review
   request on other paths, if wanted, is a new decision and gets its own
   CODEOWNERS.

3. **Accept the `write`-level trigger, and record it rather than inherit it.**
   The exposure is fleet model budget spent by a `write` collaborator on a
   public repository, bounded by the bot's own dedupe (a second request for
   the same commit does not start a second task) and by fleet quotas. It is
   accepted because the two properties that made ADR-0091's boundary
   load-bearing -- policy read from the head branch, and pull-request text
   reaching the agent as instructions -- do not apply to this bot, so what is
   left is spend, not influence. A `role_name`-based gate belongs in the bot,
   not in a workflow here, and is filed on the fleet repository as the
   follow-up.

4. **The merge gate stays "reviewed at the head commit, findings answered",
   and auto-merge stays off.** The bot pins each review to the commit it read,
   so freshness is `review.commit_id` against `headRefOid` and nothing
   looser. A comment review cannot be a required status check, which is the
   original reason `--auto` is off (#749/#750 landed with 6 real findings
   unaddressed) and it survives the change of reviewer unchanged.

   This decision does NOT settle whether a rebase invalidates a review. It
   does today, because the head changes and the commit id with it, and that
   collides with `assert-fresh-merge-base.sh` demanding a rebase whenever main
   moves: on 2026-09-10 four open PRs had exactly one review each, all at
   pre-rebase commits. Whether a review should survive a rebase that leaves
   the branch's own diff unchanged is a real question with a real argument on
   both sides, and answering it needs a durable diff hash the bot does not
   emit today. It is #1589, and until it is decided the strict reading holds
   and the status script says a review at an older commit does not cover the
   head.

5. **A missing review is five states, not one.** No trigger; a malformed
   trigger (confused reaction, no task); a task queued or running; a task
   terminal with no review posted; and a review at an older commit.
   `scripts/pr-review-status.sh` classifies the bot's task comment into
   `none`/`running`/`done`/`dead`/`unknown` and reports which, because
   "queued" and "failed" need opposite actions and nothing retries a failed
   review task.

## Consequences

Nothing in this repository authorizes, configures or runs a review any more,
so there is no repository-side surface to keep correct, and no credential in a
repository environment. One protected environment created for the old path
outlives this change: it holds a credential no workflow can reach any more,
and it should be deleted once that key is revoked at the vendor. That is an
operator action outside a pull request, and #1586 names the environment and
tracks it.

The trigger is a comment, so a reviewer's absence is silent by construction.
That is why decision 5 exists: a merge check that only asked "is there a
review" would wait forever on a failed task and clear a PR nobody had asked
about.

Trust in the review is bounded by what the review says about itself. The bot's
body names what it verified, how, and what it did not check; a review that
names its own gaps is the one worth reading first.

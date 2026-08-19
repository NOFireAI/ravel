# Contributing to Ravel

Thanks for your interest in Ravel. Contributions of code, documentation, tests, bug reports, benchmarks, and design feedback are welcome.

Ravel is a distributed observability database where object storage is the durable source of truth. Correctness, durability, and compatibility matter more than minimizing the size of an implementation.

## Bug reports

Before opening an issue, search existing issues to check whether the problem has already been reported. If it has, add your details there instead of opening a duplicate.

Confirm the bug reproduces against the current `main` branch HEAD, not an older commit, a release tag, or a fork you have not updated. A report that only reproduces on an old commit may already be fixed, and costs a maintainer the same investigation time to find that out.

A useful bug report includes:

* what you expected to happen;
* what actually happened;
* a minimal reproduction, confirmed against current `main`;
* the commit hash you reproduced it on;
* relevant logs, configuration, and environment details.

The easier a problem is to reproduce, the easier it is to fix.

## Feature requests and larger changes

For substantial features, protocol changes, or architectural changes, open or join an issue before investing heavily in an implementation.

Explain the problem first. A good proposal describes the use case, desired behavior, and important trade-offs rather than only proposing an implementation.

Some parts of Ravel are persistent contracts. Changes to segment formats, protobuf schemas, canonical identities, commit tokens, or object key layouts require an ADR and a version change. Do not modify persistent formats in place.

## Making changes

Keep pull requests focused. Avoid unrelated refactoring, formatting, dependency changes, or cleanup.

When changing behavior:

* add or update tests;
* update the relevant documentation in the same change;
* preserve Ravel's durability and consistency guarantees;
* avoid `unsafe` and `unwrap`/`expect` in production paths;
* keep exact semantics as the default unless approximation is explicitly exposed.

Read the relevant specifications and ADRs in `docs/` before changing storage, consistency, query, or protocol behavior.

For AI-assisted contributions, see [AI_POLICY.md](AI_POLICY.md).

## Building and testing

Ravel is a Rust workspace.

For fast iteration on a crate:

```bash
cargo check -p <crate>
cargo test -p <crate>
```

Before submitting a pull request, run the repository gates:

```bash
scripts/gates.sh
```

At minimum these enforce formatting, Clippy with warnings denied, and tests. The gate script also handles feature-specific checks such as SQL and Flight SQL where applicable.

Please do not submit a PR claiming tests or benchmarks passed unless you actually ran them.

## Commits

Ravel uses Conventional Commits with short, imperative subjects:

```text
feat(query): add ...
fix(ingest): handle ...
docs: clarify ...
test(catalog): cover ...
chore: update ...
```

Keep the subject to 72 characters or fewer and explain **what changed and why** in the commit body when it is not obvious.

Sign off commits:

```bash
git commit -s
```

Reference the relevant issue when applicable:

```text
Refs: #123
```

or, when the change fully resolves it:

```text
Fixes: #123
```

## Pull requests

Before opening a PR:

1. Rebase or update your branch against current `main`.
2. Review your own diff.
3. Remove unrelated changes and generated noise.
4. Run `scripts/gates.sh`.
5. Update documentation where behavior changed.

In the pull request, explain:

* **what** changed;
* **why** it changed;
* how you verified it;
* any compatibility, performance, or operational implications.

Small PRs with a clear purpose are easier to review than large mixed changes.

Review is part of the contribution process. Maintainers may ask for a different approach, additional tests, a smaller scope, or an ADR before a change can be merged.

Ravel also runs CodeRabbit, but only when a maintainer asks it to. Automated review is not triggered by opening a pull request, by a label, or by a comment, and `@coderabbitai` commands do nothing here. A maintainer dispatches a review of one revision, and CodeRabbit posts a single comment review that neither approves nor blocks. Treat its findings as one more opinion to answer, not as a gate. See [ADR-0090](docs/adrs/0090-maintainer-gated-coderabbit-reviews.md).

## Licensing

Ravel is licensed under the Apache License 2.0.

Only contribute work that you have the right to submit under the project's license. The same requirement applies to code produced with AI tools or derived from other projects.

Thanks for helping make Ravel better.

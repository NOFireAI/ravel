# Changelog fragments

Each change that needs a changelog entry adds one file here instead of editing
`CHANGELOG.md`, so two pull requests in flight never conflict on it.

- Name: `NUMBER.SECTION.md`, where `NUMBER` is the issue or pull request number
  and `SECTION` is one of `added`, `changed`, `removed`, `fixed`, `security`.
  A second change under the same number adds a suffix: `NUMBER-2.SECTION.md`.
- Content: the bullet exactly as it will appear in `CHANGELOG.md`, starting
  with `- `. No heading lines, LF line endings.
- A fix to an entry that has not been released yet edits that entry's
  fragment rather than adding a new one.

Example, `1234.fixed.md`:

```markdown
- **A refused load names the column it refused.** Previously the message
  named only the row.
```

`scripts/changelog-assemble.sh --check` validates every fragment (CI runs it on
each pull request). Release prep runs `scripts/changelog-assemble.sh` first,
which folds the fragments into `[Unreleased]` and deletes them, and only then
turns `[Unreleased]` into the `## [X.Y.Z] - DATE` section: the release notes
are read from that section, so folding after it is cut leaves the fragments
out. ADR-0037's changelog-fragments amendment lists the changelog steps of a
release in order.

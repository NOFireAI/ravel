# Changelog fragments

Each change that needs a changelog entry adds one file here instead of editing
`CHANGELOG.md`, so two pull requests in flight never conflict on it.

- Name: `NUMBER.SECTION.md`, where `NUMBER` is the issue or pull request number
  and `SECTION` is one of `added`, `changed`, `removed`, `fixed`, `security`.
  A second change under the same number adds a suffix: `NUMBER-2.SECTION.md`.
- Content: the bullet exactly as it will appear in `CHANGELOG.md`, starting
  with `- `.

Example, `2323.changed.md`:

```markdown
- **Changelog entries are written as fragments under `changelog.d/`** and
  folded into `CHANGELOG.md` before a release is tagged.
```

`scripts/changelog-assemble.sh --check` validates every fragment (CI runs it on
each pull request). Before a release tag is cut, `scripts/changelog-assemble.sh`
folds the fragments into the `[Unreleased]` section and deletes them, and the
result is committed.

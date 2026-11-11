# Guard scripts reference

Full reference for the guards under `scripts/guards/`, the PreToolUse hook
and the pre-push hook. `CLAUDE.md` lists each one in a line; read the entry
here before you rely on a guard's exit codes or its escape hatch.


Fast, dependency-free checks that fail closed BEFORE a known-expensive
mistake. Each encodes a failure class that recurs; run the relevant one
as a precondition, not after the damage.

`.claude/settings.json` is the team's, and it is where a guard that must
bind every session belongs. Personal tooling - an indexer's hooks, a
custom status line, a helper script with a machine-specific path - goes in
`.claude/settings.local.json` and `.claude/helpers/`, both gitignored.
Editing the tracked file for personal setup puts a permanent conflict
between your working tree and every change anyone else makes to it, and it
ships your paths to fleet executors. Both settings files are read and
merged, local last, so nothing is lost by splitting them.

Several of these no longer depend on being remembered.
`.claude/guards/pretooluse.mjs` runs as a PreToolUse hook and refuses the
tool call outright: a gate, a guard or a `git push` (whose pre-push hook
is a guard: its refusal is the push's exit code, and `git push | tail -3`
once reported 0 on a refused push) piped into `tail`/`head`/`grep`/`rg`/
`sed` or followed by `&& echo`, in the command itself or inside any
command substitution (`out="$(guard | tail -1)"` and its backtick
spelling run the same gate and read the same pipe's status), an
assignment to zsh's reserved `status`/`path`/
`argv`/`PWD`, a `ScheduleWakeup` under 900 s, an `Edit`/`Write`
inside the primary checkout, a bare 40-character hex literal in a Bash
command, and three destructive git operations: `reset --hard/--soft` onto
a remote ref, `filter-branch`, and a force-push naming `main`; and, in a
checkout under the fleet work root only (a dispatched fleet clone, or an
orchestrator checkout that lives there), `cargo clippy --workspace` or a
bare `cargo clippy` with no `-p`, whose replacement is `scripts/gates.sh
--tier executor -p`. The gate
patterns match cargo's global flags
too (`cargo --locked test | tail -1` was allowed until they did). It
fails open on any internal error, and it exempts a dispatched fleet clone
(which is itself the isolated workspace) from the worktree rule; the
workspace-clippy rule is the one that applies only there.
`RAVEL_GUARD_ALLOW_PRIMARY=1` is the escape hatch for the worktree rule;
`ALLOW_DESTRUCTIVE=1`, written inline at the front of the command (shell
state does not survive between tool calls), is the one for the git rules.
`RAVEL_GATES_ALLOW_UNSCOPED=1`, spelled the same way, is the one for the
workspace-clippy rule, for a deliberate unscoped run in a fleet clone.
`ALLOW_LITERAL_SHA=1`, spelled the same way, is the one for the hex rule,
and which answer is right depends on where the literal came from. A SHA
typed or finished from a prefix read off earlier output gets resolved in
the same command instead. A SHA copied from a check just run is the check,
and must be kept: a `--match-head-commit` value re-resolved at merge time
matches whatever the head is by then, so a push landing after review merges
unreviewed. That is why `scripts/pr-review-status.sh` prints its merge line
with the prefix already on it.
The same hook also allows and annotates: a raw `gh pr view`, `gh api
.../pulls/N/comments`, `gh issue view`, `pr-review-status.sh`,
`epic-status.sh`, `fleet-dispatch-intent.sh intent`, `check_docs.py`,
`df -h`, `git worktree list` or a control-plane `curl` gets a note naming
the `bin/agent-cli` subcommand that answers it in one call, only when the
checkout has that file. The table is
`.claude/guards/agent-cli-nudges.json`, shared with `bin/agent-cli usage`,
which counts those raw commands against agent-cli calls in this machine's
session transcripts; that count, not the note, says whether the tool is
being used.
Its cases live in `.claude/guards/pretooluse.test.sh` and run in CI's
doc-scripts job; add one there before changing a rule.

`.githooks/pre-push` is the same protection at the git level, and it
covers what the hook cannot see: it refuses a push that would drop
commits from `main` or from any branch with an open pull request, prints
the commits and diffstat that would be lost, and takes the same
`ALLOW_DESTRUCTIVE=1`. It never sees `--force` (git passes a pre-push
hook four fields on stdin and no flags), so it tests ancestry instead,
which also catches `--force-with-lease` and a `+refs/` refspec. Install
it once per checkout with `scripts/guards/install-git-hooks.sh`
(`--check` reports whether it is installed); `core.hooksPath` is
per-repository config shared by every linked worktree, so one run covers
them all. A pull-request lookup that cannot be answered refuses rather
than passing for "no pull request open".

- `scripts/guards/assert-no-duplicate-dispatch.sh --issue <n> [--paths a,b]
  [--base-ref REF]`:
  refuse to dispatch work someone is already doing. Exits 65 when an open or
  recently-merged pull request already closes the issue with a closing
  keyword (`Fixes: #N`; a bare citation such as `Refs: #N` is a note) (skip
  and log, never open a second one), 66 when an OPEN, non-draft pull request
  is already touching a predicted file, line range or definition (a draft,
  or a merged or closed pull request on the same file, is printed under its
  own heading as context, not a refusal: a held draft once blocked two
  epics' tickets for a day, #2749), 64 on usage, including a `--paths`
  entry that does not parse or is empty after trimming, and 69 when the
  question could not be asked (an OPEN, non-draft pull request's files and
  patches or its merge base could not be fetched, or the base ref does not
  resolve, among others). Only a question that can change the verdict may
  refuse: a draft, merged or closed pull request is matched against every
  entry as the whole file, its merge base is never asked for, and a files
  list it could not read is a NOTE (#3463: a merged pull request whose base
  branch was deleted made the compare endpoint 404 and refused every
  dispatch over its files). A `--paths` entry (and so a `DISPATCH_PATHS`
  entry) takes one of three forms: `path`, the whole file, where any change the pull
  request makes to it collides; `path:L10-L80`, a base line range, which
  collides only when one of the pull request's patch hunks for that file
  changes a base line inside it; and `path#symbol`, a definition on the
  base, which collides only when a hunk changes a base line inside it.
  Entries are separated by commas. Each entry is trimmed of leading and
  trailing whitespace; an entry empty after trimming, including one left
  by a leading, trailing or doubled comma, is a usage error, and so is a
  `--paths` value containing a newline or carriage return, each refused
  before any gh call. A changed base line is a
  removed or replaced line; a pure insertion counts against the base lines
  on both sides of it; context lines do not count. A range or symbol entry
  whose file is not on the base, and a range that starts past the base
  file's last line, are compared as the whole file, with a NOTE. The
  symbol anchor is plain text, not a parser, and applies to `.rs` files
  (the Rust forms) and `.sh` and `.bash` files (the shell forms): a line
  that defines the name with fn, struct, enum, trait, mod, const, static,
  type or union, or an impl line that names it anywhere in its header, in
  Rust; a function definition in shell; through its matching close brace
  (the script's header lists the qualifiers and the fallbacks when braces
  do not balance). A symbol entry on any other file type, `docs/` markdown
  aside, is compared as the whole file with a NOTE, and so is a name not
  found on the base. A `.md` file under `docs/` is compared by heading
  section instead of by line: a line's section is its nearest preceding
  ATX heading outside fenced code, and an entry collides only when a hunk
  changes a line in one of the entry's sections. A fence closes only on a
  fence of the same character (backtick or tilde) at least as long as the
  one that opened it. `path#symbol` names a heading by its GitHub anchor
  (`docs/x.md#usage` for "## Usage") and covers its nested subsections;
  `path:L10-L80` covers every section with a line in the range. A bare
  `docs/` path names no section and keeps the whole-file rule. It reads
  each candidate's files and patches from the paginated files endpoint in
  one call, because `gh pr list --json files` caps at 100 and would report
  a wide pull request as touching nothing. That endpoint returns at most
  3000 files, so a pull request listing 3000 or more may touch more than
  it lists and is taken as colliding with every entry, with a NOTE. A file
  the pull request renamed matches an entry naming either its old or its
  new path, and its patch is numbered on the old file. Symbols, headings
  and sections are read from `--base-ref` (default `origin/main`), while a
  pull request's hunks number lines on its own merge base. A range, symbol
  or section entry is compared by line only when all of these hold for the
  matching file; otherwise that file is compared as the whole file, with a
  NOTE naming the file and the reason. The file has a patch: one absent or
  empty (a diff too large to inline) is not compared by line. The pull
  request neither adds nor deletes the file: status added or removed, or a
  hunk header with an empty side (`-0,0` or `+0,0`), is not. The merge
  base is known and present locally. It is read once per pull request, and
  only when an entry needs it, from the compare endpoint
  (`repos/OWNER/REPO/compare/BASE...HEAD`, BASE the pull request's base
  branch and HEAD its head commit, `.merge_base_commit.sha`). A listing
  with no base branch or head commit, a response naming no merge base, or
  a merge base this checkout does not have (it is not fetched) each fall
  back. A gh failure on that call exits 69. The file on the merge base
  (its old path, for a rename) is the same blob as the entry's file on
  `--base-ref`. Any difference, wherever it is in the file, means it
  changed since the pull request forked. Every context and removed line
  of each hunk equals the base file's line at the number its hunk header
  gives it, and every hunk has at least one such line to check. It is the
  pre-dispatch half of the problem;
  `check-duplicate-work.sh` compares patch-ids between pull requests that
  already exist, which a task with no diff yet cannot use.
- `scripts/guards/assert-green-head.sh <pr> [--rerun]`: the merge gate. Green
  means green on the pull request's current head: the head SHA is re-read
  after the query and a move aborts the verdict, and an empty rollup is
  reported as no-verdict rather than as zero failures. On red it records a
  failure signature (failing job, failing step, test identifiers from the
  failed log) against that SHA in the orchestrator state, reruns once, and
  compares: the same signature twice exits 1 (a real failure, escalate), two
  different signatures exit 4 (a flake, budget spent). A cancelled or
  timed-out check exits 6 and goes to `ci-sweep-cancelled.sh`, which owns the
  timeout-versus-supersede distinction.
- `scripts/guards/show-destructive-loss.sh <target-ref> [--from <ref>]
  [--hard]`: prints the commits a reset would orphan, their combined
  diffstat, and (with `--hard`) the uncommitted work that would go with no
  reflog entry. Run it before setting `ALLOW_DESTRUCTIVE=1`; that is what the
  flag is meant to be read beside.
- `scripts/guards/check-test-suites-run.sh [--list]`: every tracked
  `*.test.sh` must be named on a non-comment, non-`name:` line of some
  workflow, or be listed as an exception carrying a reason. Exit 1 names the
  orphans; exit 2 is "could not check" (no suites found at all, no workflows
  directory, an exception with no reason), which is deliberately not a pass.
  The two halves of the test tree fail differently: `make test-python` uses
  `unittest discover -p 'test_*.py'`, so a Python suite cannot be orphaned and
  grepping a workflow for its filename proves nothing, while shell suites are
  enumerated by hand and a new one runs nowhere until somebody remembers.
  That has bitten three times, each a green suite that had never executed
  sitting beside a real defect. Runs in `gates.sh` as well as CI, because the
  person it exists for is the one who just added a suite. It does not see
  suites named by another convention. `deploy/metricsbench/tests/` holds one
  of each: its `.test.sh` file is scanned and its `.sh` sibling is not, though
  both are wired by hand. It reads only the `*.yml`/`*.yaml` directly under
  `.github/workflows`, since a README or a disabled file there is not
  something that runs a suite.
- `scripts/guards/assert-worktree.sh`: exits non-zero if the cwd is the
  PRIMARY checkout rather than a linked worktree. Run it before the first
  edit/commit of any isolated unit of work. A concurrent session can hold
  in-flight state on the primary checkout at any moment, and an edit or a
  stray `git checkout -- .` / `git reset --hard` there clobbers it
  silently; the workspace-isolation rule has no doc-only/one-file
  exception.
- `scripts/guards/check-disk-headroom.sh [dir] [min_gb]`: exits non-zero
  when the volume backing `dir` has less than `min_gb` free (default 20).
  Run it before any cold `--all-targets`/`--workspace`/feature-lane build
  or verify-dispatch gate: ENOSPC surfaces mid-link as a FAKE
  `linking with cc failed` (errno 28) that reads as a code bug, and a full
  disk can break the harness's own output capture so nothing runs at all.
  `FLEET_DISK_REAP=1` auto-runs `disk-reap.sh -y` when below the floor.
  Exit 1 is "below the floor, or the path is unreadable"; exit 2 is an
  argument it cannot use, which today means a directory argument that was
  passed and is empty. That is a caller whose variable did not survive,
  not a caller asking for the default, and `${1:-.}` cannot tell the two
  apart: it measured the current directory and passed about a volume the
  caller was never going to write to.
  The check proves headroom at one instant, not for the duration of the
  build. A session that read 82 GB free, started a cold gate, and took
  the volume to 886 MiB still passed this guard: another session was
  building at the same time. For anything longer than a few minutes,
  also arm `scripts/guards/disk-watchdog.sh <your-worktree>` alongside it.
  Killing your build costs a retry; letting the volume reach zero stops
  every Bash command in every session on the host, including the ones
  that would clean up.
- `scripts/guards/disk-watchdog.sh <scope-dir> [floor_gb] [warn_gb]
  [sample_s]`: samples free space and kills the build processes whose cwd
  is inside `scope-dir` once it drops below the floor
  (default 9 GB, warning at 15). The scope is REQUIRED and it is what
  makes the thing safe here: a watchdog that matches by command name
  kills every session's build on this box and reports it as killing
  yours. Two sessions wrote that version independently within one hour on
  2026-09-09 and one killed the other's compile with it, which is why
  this exists as a script rather than as the paragraph above.
  The matched set is `cargo` and `rustc` plus the linker children they
  spawn (`cc`, `ld`, `collect2`, `rust-lld`, `clang`, `clang++`): SIGKILL
  to rustc leaves its linker running, and linking is the phase writing
  the largest artifacts, so a matcher of the first two reports success
  while the volume keeps draining. After the kill it re-scans and kills
  again until the scoped set is empty, since a child can outlive the
  parent that was matched, and gives up with exit 1 after five rounds
  rather than claiming a volume it did not free.
  `WATCHDOG_DRY_RUN=1` names the pids and kills nothing; use it rather
  than testing the kill path against live processes. On firing it writes
  `<scope-dir>/.disk-watchdog-fired` BEFORE killing anything: a gate whose
  run overlaps that marker is INVALID rather than red, because a runner
  reports a SIGTERM'd test as a failure and the next reader cannot tell
  the two apart. Arming clears a marker from an earlier run so its
  presence always refers to the current one; a dry run arms nothing and
  leaves it alone, because the reason to run one is usually a gate that
  just came back red and the marker is the answer. It never touches the
  gate's own exit code. Cases in
  `scripts/guards/disk-watchdog.test.sh`.
- This host is itself a fleet executor. `~/.fleet/executor` holds the
  cargo target of whatever task it has claimed from the queue, which no
  session here chose and which grows without warning; it has been observed
  at 46 GB. So "no other session is building" does not mean the volume is
  idle, and a gate failure is not evidence about the crates you scoped.
  Measure before concluding: the main suite and every feature lane of one
  gates.sh run write into one `target/`, and nothing prunes between them.
  Measured on 16-core amd64 executors (#3140), for closures that reach the
  sql, flight-sql, esql and ravel-bench lanes: a `-p ravel-server` gate
  finished at 93 GB in one run and 186 GB in another; a `-p ravel-sql -p
  ravel-server -p ravel-bench` gate filled its volume at 188 GB (144 GB of
  `debug/` from the feature lanes, 44 GB of `ci/` from the ci-profile main
  suite) and died at link before its flight-sql lane finished, so that
  closure needs more than 188 GB. Those figures predate the `agents` and
  `mcp` lanes, which add two more ravel-server feature builds to the same
  `target/`; a ravel-server closure now needs more than they say, by an
  amount not yet measured.
- A cold gate needs 54-70 GB of `target/` per worktree when its closure
  runs no feature lane, and from about 93 GB to more than 188 GB when it
  reaches the sql, flight-sql, esql and ravel-bench lanes, so this machine
  runs ONE at a time regardless of how many sessions are otherwise idle.
  Two sessions gating concurrently is the expected failure, not bad luck:
  say so on the shared channel and sequence, or push the gate to PR CI
  with `FLEET_MERGE_SKIP_GATES=1` (which still needs a valid receipt, so
  it moves the gate rather than skipping it). This is unrelated to the
  fleet dispatch concurrency cap: fleet executors build on their own
  remote host and cost nothing locally.
- `scripts/guards/assert-fresh-dispatch-ref.sh <ref-sha>`: exits non-zero
  unless `<ref-sha>` is the tip of `origin/main` fetched in THIS
  invocation. Run it against the ref you are about to `fleet_dispatch`: a
  SHA read earlier in the session goes stale the moment another PR merges,
  and dispatching it silently rebuilds on a superseded tree.
  `ALLOW_STALE_REF=1` to dispatch an intentionally older ref.
- `scripts/guards/check-duplicate-work.sh <pr-number>`: reports open PRs
  whose work overlaps this one, BEFORE you start on it. Exit 0 = nothing
  overlaps, 1 = something does, 2 = could not tell. `IDENTICAL` means two
  PRs make the same change, matched by `git patch-id --stable` over each
  diff, so it catches a duplicate whatever its title, branch or author says;
  `OVERLAP` means a shared file, which is normal and only tells you whose
  toes you are near. It reports rather than refuses: which PR should close,
  or in what order two should land, depends on facts a script cannot see.
  Several Claude sessions work this repo through one shared `gh` account, so
  a PR's author never says which session owns it and idle time says nothing
  about whether that session is alive. Both assumptions failed on
  2026-09-14: #1786 and #1788 bumped rustls for the same advisory twenty
  minutes apart under titles too different to match by eye, and sat in the
  merge queue together, where the second would have rebased to an empty diff;
  and five fix rounds went out against PRs whose session was awake and
  already fixing the same findings, on heads that moved minutes later, so
  landing one would have reverted the other session's work while looking
  like an ordinary landing. Run this before the first action on any PR that
  is not demonstrably yours, and re-resolve the head immediately before a
  `fleet_dispatch` against it: findings extracted against a head that has
  since moved are already stale. Claim a PR by commenting on it BEFORE
  reading its findings, not after dispatching a round; a claim posted at the
  end of the pipeline documents an intention events have already overtaken.
  Cases in `scripts/guards/check-duplicate-work.test.sh`; the IDENTICAL path
  is tested synthetically because its only real instance was closed within
  the hour.
- `scripts/guards/assert-fresh-merge-base.sh <pr-number>`: exits non-zero
  when a PR's merge base is behind `origin/main`, printing how far and what
  it has not seen. **Since 2026-09-13 a merge queue on `protect-main` is what
  actually enforces freshness**: it rebases each entry onto current main and
  runs full CI on the combined result before landing, so do NOT hand-rebase a
  PR merely because main moved, and do not treat a behind-ness count as a
  reason to pay a CI cycle. The queue is `REBASE`/`ALLGREEN`, batches up to 5,
  and `ci.yml` already carried the `merge_group:` trigger. This guard remains
  the check for a merge that BYPASSES the queue, and for diagnosing a branch
  that will not enter it. Green CI on a stale base is not evidence about the merge: a
  PR green against an older base can still break `main` (8a534f43 left main
  uncompilable exactly this way, a five-argument call site landing minutes
  before another PR made the function take six, both PRs green), and a gate
  added to `main` after a PR went green has never run against that PR at
  all. Four PRs were CI-green and behind simultaneously on 2026-09-06.
  `main` DOES run its own CI (`ci.yml` has `on: push: branches: [main]`;
  the coverage lane and the publish gate both need it), so a break of the
  8a534f43 kind is detected, but only once the merge has landed and anyone
  pulling in between has a broken tree. Detection after the damage is not
  prevention. One class survives main's push CI entirely and this guard is
  its only detector: a landing loop that copies files against a list
  derived from current main can DELETE a change another session landed
  minutes earlier, and that revert is textually clean and green, because
  removing lines compiles as well as never adding them and the tests for
  the reverted work went in the same commit. Two were caught on 2026-09-09,
  both only because the guard printed a merge-base the author did not
  recognise. `ALLOW_STALE_MERGE_BASE=1` to
  proceed anyway. It exits 1 for a base that is behind and 2 when it could
  not tell (bad argument, no such pull request, git failed), so a caller
  reporting to a human can say which it got; a caller that only needs "may
  I merge" treats any non-zero as no, and MUST, because 126 and 127 come
  from the shell rather than the guard and mean it never ran. That is not
  hypothetical: the guard is newer than most checkouts, a primary checkout
  here is routinely dozens of commits behind, and one lacking the file
  exits 127, which a caller switching on 1 versus 2 falls straight
  through. Run it from a worktree that is on current main; asking a stale
  tree whether a branch is stale is the same mistake one level up. Note it fetches with an explicit destination refspec: a
  hand-rolled version using `git fetch origin main <other-ref>` and then
  `rev-parse FETCH_HEAD` reads back MAIN, so it compares main with itself
  and reports every branch fresh.
- `scripts/guards/assert-struct-literals-current.sh <branch> [base]`:
  refuses a branch whose change to a struct's or braced enum variant's
  field set (a field added, removed, or its type or visibility changed; a
  reorder is not a change) or to a function's parameter count, `self`
  receiver or parameter types (not names) breaks a `Name {` or
  `Enum::Variant {` literal or a `name(` call that landed on `[base]`
  (default `origin/main`, fetched with an explicit destination refspec)
  after the branch forked. A post-fork `.rs` file the branch does not touch
  is searched in full; one the branch also touches is searched only on the
  lines `[base]` added to it after the fork, since the branch already
  updated that file's older sites, and a literal `[base]` appended beside
  the branch's own edit rebases cleanly and breaks only in the queue. A
  call counts only when its argument count fits an old signature (one the
  branch removed, or any when it only added one; the receiver is not
  counted for a `.name(` call), so
  `Vec::new()` does not trip a changed `fn new(limit: u64)`; a call it
  cannot count on one line (it wraps, or takes a closure) is reported marked
  "(uncounted)". Exit 1 names each file, line and landing commit and says to
  rebase and update the site; 0 prints one line saying how many structs and
  functions it checked against how many post-fork files, split into those
  searched in full and those searched on their added lines; 2 is could-not-answer (a git
  failure, an unknown ref), never 0; 64 is usage. Both halves of this break
  are green on their own base and it surfaces only when the merge queue
  rebases, at the cost of a CI cycle and a review round. Two instances on
  2026-10-11: #2804 made `ServerConfig.audit_pipeline` an `Option` while
  #2809 landed a literal of the old shape (E0308), and #2842 gave
  `set_host_memory_gate` a third argument while #2841 landed six
  two-argument calls (E0061). It is textual and over-approximates on
  purpose: a pattern match on `Name {` or a different function sharing the
  identifier and an old argument count is reported too, which costs one
  grep, while a miss costs a queue cycle; renames, tuple structs, and tuple
  or unit enum variants are out of scope. `scripts/pr-review-status.sh`
  runs it for the pull request head before the merge line: exit 1 withholds
  the line, and `ALLOW_STALE_SHAPE_SITES=1` (for hits verified by hand as
  false positives) prints them with a note that the guard was overridden,
  then the line; any other non-zero prints a warning and keeps it.
  `scripts/fleet-result-merge.sh` runs it after the gates and before its
  push, and refuses on any non-zero exit, exit 2 included. Cases in
  `scripts/guards/assert-struct-literals-current.test.sh`, run by
  `gates.sh` and CI's doc-scripts job.
- `scripts/guards/assert-gh-auth.sh [hostname]`: exits non-zero when gh
  cannot complete an authenticated API call. Run it before any landing
  sequence: tokens die mid-session, and a failure found at push time
  strands committed work. `fleet-result-merge.sh` runs it first.
- `scripts/guards/assert-clean-authorship.sh <ref> [email]`: exits
  non-zero if any commit on `<ref>` not yet on origin/main has an author,
  committer, or Signed-off-by other than the expected identity (default:
  `git config user.email`), or carries an AI attribution trailer. The
  merge script runs it after its rewrite; run it yourself after any
  manual amend or script-bypassing merge. A wrong identity on protected
  `main` cannot be fixed later.
- `scripts/guards/check-guarded-sql-parse.sh [path ...]`: exits non-zero when
  anything under `crates/ravel-sql/src/` reaches a SQL parser front end
  (`DFParser`, `DFParserBuilder`, sqlparser's `Parser`) outside
  `complexity_guard::parse_guarded`, which runs the pre-parse complexity guard
  and then parses. Every parse of caller text needs that guard, because the
  walks over the parsed tree abort the process on a statement the parser's own
  recursion limit does not bound, and as a convention it failed on two of the
  three parse sites (issue #1760). Test code that parses fixture text carries
  `// guarded-parse-allow: <reason>` on the line or in the comment block above
  it; the reason is required. Exit 1 is a finding, 2 is a missing or moved
  anchor, which is the case where the scan would otherwise pass everything.
  Wired into `gates.sh` and CI's doc-scripts job; cases in
  `scripts/guards/check-guarded-sql-parse.test.sh`.
- `scripts/guards/check-guarded-esql-parse.sh [crate-root caller-root ...]`:
  exits non-zero when ES|QL source text could enter `ravel-esql` other than
  through `ravel_esql::parse` and `ravel_esql::lex`, the two entries that apply
  the byte and token limits (and, for `parse`, the nesting and stage limits)
  before the recursive parser runs (issue #2892). Under
  `crates/ravel-esql/src/` it refuses a bare-`pub` function, a `pub trait`
  method, or a `FromStr`/`From`/`TryFrom` impl that takes `str`, `String`,
  `[u8]`, `Read` or `BufRead`, or lexed tokens (a slice, `Vec` or iterator of
  `Token`, by any path), since a function over tokens would run the parser
  without the nesting and stage limits; a bare `pub` in a private module
  counts, so internal helpers say `pub(crate)`. Under `crates/ravel-sql/src/`
  and `services/ravel-server/src/` it refuses any path through the crate's
  `lexer` or `parser` module that does not end at `parse`, `lex`, or an alias
  the crate declares of one of them (`lexer::tokenize`). Its suppression
  marker is its own, `// guarded-esql-parse-allow: <reason>`, with the reason
  required; the SQL guard's marker does not silence it. Exit 1 is a finding; 2 is a missing anchor (`pub fn parse`
  in parser.rs or `pub fn lex` in lexer.rs gone, duplicated, or no longer
  read as taking text, or no caller naming either), a missing default root,
  or no sources. Wired into `gates.sh` and CI's doc-scripts job; cases in
  `scripts/guards/check-guarded-esql-parse.test.sh`.
- `scripts/guards/check-promql-unreachable.sh [path ...]`: exits non-zero when
  an `unreachable!` under `crates/ravel-promql/src/` carries no
  `// unreachable-allow: <arm> -- <reason>` marker, on its own line or in the
  contiguous comment block above it, with a reason that starts on the marker
  line itself (it may continue on the lines below; a marker whose reason
  begins on a later line is a finding). Those arms used to rest on
  promql-parser's own checks, a third-party guarantee on a caret version
  range; issue #1701 converted every arm a parsed query could reach if those
  checks relaxed to `Error::Unsupported` and left only the arms an exhaustive
  prior match in Ravel's own code narrows out of reach, each marked with the
  arm that now rejects first. Exit 1 is an unmarked or empty-reason finding, 2 is zero
  `unreachable!` occurrences found or a missing source directory, so a rename
  or move cannot silently turn this into a no-op. Wired into `gates.sh` and
  CI's doc-scripts job; cases in `scripts/guards/check-promql-unreachable.test.sh`.
- `scripts/guards/check-release-profile.sh`: exits non-zero when the published
  binaries would not be built optimised. `[profile.release]` must set `lto` to
  `"thin"`, `"fat"` or `true`, `codegen-units = 1`, and keep debug info on
  (the published debug-symbols image is split out of these binaries), with no
  `opt-level` other than 3 there or in a per-package release override, in
  either the `[profile.release.package.NAME]` or the inline-table spelling;
  every `cargo build` in the root `Dockerfile` must carry `--release` and no
  `--profile`, checked per build after joining continuation lines; neither that
  file nor `publish-images.yml` may set a `CARGO_PROFILE_RELEASE_*` variable;
  and `publish-images.yml` must build the root `Dockerfile`. Test lanes are
  free to tune `profile.ci`; this is the profile that must not follow them.
  Exit 1 is a finding, 2 is a missing anchor (no `[profile.release]` table, no
  `cargo build` line, no build step in the publish workflow), which is the
  case where the scan would otherwise pass everything. Wired into `gates.sh`
  and CI's doc-scripts job, cases first; cases in
  `scripts/guards/check-release-profile.test.sh`.
- `scripts/guards/check-quick-xml-entry-points.sh [Cargo.lock] [deny.toml]`:
  exits non-zero unless deny.toml's RUSTSEC-2026-0194/-0195 ignore comment
  names every direct Cargo.lock parent of an affected quick-xml version,
  each together with its OWN version as it appears in Cargo.lock (e.g.
  "object_store 0.13.2"), so a parent bumping to a version that newly
  resolves an affected quick-xml fails against a comment still naming the
  old one. The affected set (every quick-xml version older than 0.41.0,
  the release that fixes both advisories) is derived from Cargo.lock
  itself, not hardcoded. Two live parents pull them in today, inferno
  0.11.21 (quick-xml 0.26.0, behind the off-by-default `profiling`
  feature) and object_store 0.13.2 (quick-xml 0.39.4, entering through
  object_store's own `aws` feature, which ravel-bench turns on directly
  behind its own off-by-default `parquet-baseline` feature, not through
  parquet's unrelated `object_store` feature), and the comment is the
  only place recording which binaries the ignore actually rests on; a
  rename or a third parent showing up must fail loud rather than leave
  the comment stale. Matching is anchored on both the parent name and its
  version, not a bare substring test, so a crate named e.g. "store" is
  not read as documented by a comment that only names "object_store
  0.13.2". Exit 1 is a parent (or parent version) missing from the
  comment, 2 is zero quick-xml parents found at all (the anchor moved, or
  every affected version dropped out of the lock) or the
  `"RUSTSEC-2026-0194"` line itself missing from deny.toml, 64 is bad
  usage, 70 is the underlying Cargo.lock scan itself failing.
  `scripts/guards/check-quick-xml-shipped-reachability.sh` is the
  companion check that the four Dockerfile release builds never reach any
  affected version at all, via `cargo tree -i`, first proving each
  affected version resolves SOMEWHERE in the workspace (`cargo tree
  --locked --workspace --all-features -i quick-xml@<version>` must exit
  0) before trusting any per-build "not reachable" result as a pass: a
  version that resolves nowhere (a stale or mistyped entry) exits 70
  rather than reading every build as clean. The guard itself is wired
  into ci.yml's supply-chain job and supply-chain-nightly.yml instead of
  gates.sh, since it needs the cargo toolchain and a real dependency
  graph, not just a text scan, but its own cases
  (`scripts/guards/check-quick-xml-shipped-reachability.test.sh`, driven
  by a stubbed `cargo` on PATH) are cheap and run in both `gates.sh` and
  CI's doc-scripts job, same as the entry-points guard's own cases in
  `scripts/guards/check-quick-xml-entry-points.test.sh`.
- `scripts/guards/check-workflow-permissions.sh [dir ...]`: exits non-zero
  when a workflow under `.github/workflows/` declares no top-level
  `permissions:` block, or when that block itself grants a write scope. The
  repository default workflow permission is write, so a workflow without a
  floor compiles the workspace with a read-write `GITHUB_TOKEN` in the
  environment of every dependency build script cargo runs. A job that
  genuinely needs more declares it on itself; `# workflow-permissions-allow:
  top-level-write -- <reason>` above the key is the escape hatch for the
  write rule only, and there is none for a missing block. A third rule,
  `checkout-persists-credentials`, requires every `actions/checkout` step to
  set `persist-credentials: false`: a checkout that leaves the token in
  `.git/config` hands it to every later step in the job, including the build
  scripts cargo runs. Its escape hatch is
  `# workflow-permissions-allow: persist-credentials -- <reason>` on the line
  or in the comment block above it, and the reason is required, as it is for
  the write rule. The step bound is the enclosing list item, so the common
  `- name:`/`uses:`/`with:` form is read correctly. A scan that finds
  no workflow file exits 64 rather than reporting clean, so a moved
  directory cannot turn the guard into a no-op. Wired into `gates.sh` and
  ci.yml's `doc-scripts` job, cases first. Cases in
  `scripts/guards/check-workflow-permissions.test.sh`.
- `scripts/guards/check-changelog-touched.sh <base-ref> [head-ref]`: exits 1
  when a `feat`/`fix` commit in the range touches `crates/` or `services/`
  while `CHANGELOG.md` goes untouched across it, and no commit carries a
  `Changelog: none` trailer (that exact spelling, on its own line). Exit 2 is
  could-not-answer (missing argument, an unresolvable ref, a git failure), which
  is never a pass. The range is taken from the MERGE BASE of the two refs, not
  from the base ref directly: with a two-dot range a `CHANGELOG.md` edit that
  landed on the base branch after the fork point reads as this range's and
  exempts a pull request that touched no changelog. Since the guard makes
  nearly every merged pull request touch `CHANGELOG.md`, that divergence is the
  common case, not a rare one. The two exemptions are the trailer and a
  changelog edit genuinely inside the range; there is no grace period and no
  branch allowlist, because a guard with a cutoff stops being one. Wired into
  ci.yml's `doc-scripts` job with the pull request's base SHA. Cases in
  `scripts/guards/check-changelog-touched.test.sh`.
- `scripts/guards/check-amendment-integrity.sh [dir]`: exits non-zero when an
  ADR amendment heading's own marker claims something the document does not
  back up: a named section that does not carry the amendment's pointer, or a
  phrase the amendment declares retired still appearing elsewhere
  unqualified. An amendment section makes this kind of claim about its own
  effect ("the role table, section 2 and section 3 now carry an inline
  pointer to this amendment") and review kept finding the edit only reached
  one of the named places (issue #1985); this checks the claim against the
  document instead of trusting the prose. An amendment heading is any
  heading below the title whose text contains a word starting "amend" or
  "correction", in any case, at any level, since every narrower rule tried
  here skipped a real amendment or correction silently; one nested inside
  another amendment's block belongs to that block. `docs/adrs/README.md`, "Amending an ADR", is the author-facing
  marker syntax with one example per kind, and the guard's own failure
  output points there. Exit 1 is a finding: a named section without its
  pointer, a retired phrase still standing unqualified, or an
  `amendment-applies: none` with no `reason=` (`none` turns the checks off,
  so it is the marker that has to justify itself). Exit 70 is a claim that
  could not be checked at all: an amendment heading with no marker, a marker
  line that does not parse (wrapped, or misspelled), a marker missing
  `sections=`, `pointer=` or `phrase=`, an empty `sections=`, a
  named section heading that does not exist or that exists more than once,
  no such directory, no ADR files (`docs/adrs/*.md` less `README.md`, which
  is the index), or zero amendments scanned, since a scan that could not run
  and a clean tree are different answers. Exit 64 is bad usage, including a
  directory argument outside the repository. Wired into
  `gates.sh` and ci.yml's `doc-scripts` job, cases first. Cases in
  `scripts/guards/check-amendment-integrity.test.sh`.
- `scripts/guards/check-adr-band-sources.sh [dir]`: exits non-zero when a
  row of an ADR band table carries no source. A band table is a markdown
  table outside fenced code whose header row names the whole word band,
  bar, target or miss, in any case; its basis cell is the one under a
  column named basis, source, measured or evidence, else the row's last
  cell. That cell must hold an issue comment link or `comment <id>`, an
  `s3://` or `bench/` path, a `path.ext:N` citation, `measured
  YYYY-MM-DD`, or the word predicted. Specs copy these bands as the bar a
  task must clear (ADR-2867 change 6, issue #2879), and a band nobody can
  trace back is a bar nobody can check. `docs/adrs/README.md` is the index
  and is skipped. Exit 1 names the file, the table's header line and the
  row's first cell. Exit 70 is a scan that could not run: no such
  directory, no ADR files, no band table anywhere, or a band header with
  no data rows. Exit 64 is bad usage. `<!-- band-sources-allow: <reason>
  -->` on the nearest non-blank line above a table's header exempts that
  table only; an empty reason, a marker that does not parse, and a marker
  with no band table below it are each a finding. Wired into `gates.sh`
  and ci.yml's `doc-scripts` job, cases first. Cases in
  `scripts/guards/check-adr-band-sources.test.sh`.
- `scripts/check-injected-clock-helpers.sh [file]`: exits non-zero when an
  injected-clock test helper contains `thread::sleep`, `tokio::time::sleep`,
  a bare or aliased `sleep()` call, `tokio::time::timeout`, `Instant::`,
  `.elapsed()`, or `SystemTime`. A helper is any function in the
  `#[cfg(test)]` module whose signature or body mentions an injected-clock
  type (`TestClock` or `FixedClock`), plus the two helpers issue #1260 names
  by name so a rename of a clock type cannot silently empty the scan. It is
  bypassable per line with a trailing `// allow-wall-clock: <reason>`
  comment; the reason is required (an empty reason does not suppress), and
  the marker is matched on the string-stripped line so a string literal
  containing the marker text cannot suppress a real finding. Wired into
  `gates.sh` and into `.github/workflows/ci.yml` (its own cases run first,
  so a suite broken into always-passing fails CI rather than going quiet).
  Run it after touching that helper region: a wall-clock wait smuggled back
  into a clock-driven test reads as a real defect on a loaded machine, then
  as flakiness on a quiet one, and both readings cost a full gate rerun
  before anyone thinks to look at the test's own timing model. An injected
  clock's own `sleep`/`elapsed` is exempt (a receiver chain naming a clock
  type, or an identifier containing "clock"); any other receiver still
  reports. A scan that finds zero helpers is itself a failure, and so is a
  scan of the default target that finds fewer than 20 (a clock-type rename
  would otherwise empty the scan silently, and dropping one clock type from
  the predicate would silently narrow it from 26 helpers to 5).
  Scope: the default target is the single file
  `services/ravel-cli/src/load.rs`; the rule is NOT yet enforced
  workspace-wide. Pointing it at every other `.rs` that mentions an
  injected-clock type surfaces 9 findings across 5 files (in ravel-cache,
  ravel-maintain, and ravel-server; a mix of idle-eviction sleeps that poll
  a clock-controlled state and `tokio::time::timeout` deadlock guards that
  would each need a real allow-wall-clock reason), and it finds no helper at
  all in 78 more clock-mentioning files, 70 of them integration tests: 60
  under `crates/*/tests/` and 10 under `services/*/tests/`. Issue #1278 is
  the follow-up that widens the scan and resolves those.


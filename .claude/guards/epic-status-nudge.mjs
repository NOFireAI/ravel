#!/usr/bin/env node
// Nudges a session toward `scripts/epic-status.sh` instead of hand-rolled
// `gh issue view`/`gh pr view` reconciliation (issue #613). Two modes,
// wired to two different hook events because only one of them can safely
// inject context:
//
//   track  (PostToolUse, matcher "Bash"): counts hand-rolled reconciliation
//          calls since the last epic-status.sh run, in a small local state
//          file. Never blocks anything -- PostToolUse cannot deny a call
//          that already ran.
//   nudge  (UserPromptSubmit): reads that counter and, past a threshold,
//          writes a one-line reminder to stdout. UserPromptSubmit's stdout
//          becomes additional context for the next turn; PreToolUse has no
//          equivalent non-denying path in this harness, which is why this
//          is not folded into pretooluse.mjs.
//
// State lives in a local, gitignored file. A missing or corrupt state file
// degrades to "count starts at zero", never an error and never a block --
// this hook can only ever add a reminder, not stop a tool call.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
// Overridable so the test script can point at an isolated file instead of
// this checkout's real counter; unset in every real invocation.
const STATE_PATH =
  process.env.EPIC_STATUS_NUDGE_STATE ??
  resolve(HERE, "..", ".epic-status-nudge-state.json");
const NUDGE_THRESHOLD = 8;

// Matches an ad hoc reconciliation read: `gh issue view`, `gh pr view`, `gh
// issue list`. Not `gh issue create`/`comment`/`close` and not `gh pr
// create`/`merge` -- those are real actions, not a stand-in for
// epic-status.sh. Anchored to the START of a command fragment (see
// commandFragments) rather than searched anywhere in the string, so a commit
// message or echo that merely mentions either phrase doesn't move the
// counter -- only an actual invocation does.
const RECONCILE_CALL = /^gh\s+(issue|pr)\s+(view|list)\b/;
const EPIC_STATUS_RUN = /^(\.\/)?scripts\/epic-status\.sh\b/;

// Splits a shell command on real command separators and strips a leading
// `cd ... &&` or env-var assignment from each piece, so "cd x && gh issue
// view 1" and "FOO=bar gh issue view 1" still match at the START of their
// fragment. Mirrors pretooluse.mjs's HARMLESS_PREFIX approach, scaled down:
// this only needs to avoid false positives/negatives on a soft nudge, not
// close every shell-quoting escape hatch a security gate would.
const CMD_SEPARATOR = /&&|\|\||[;|\n]/;
const HARMLESS_PREFIX =
  /^(\s*(cd\s+[^&;|]+&&|[A-Za-z_][A-Za-z0-9_]*=\S+)\s*)+/;

function commandFragments(command) {
  return command
    .split(CMD_SEPARATOR)
    .map((fragment) => fragment.replace(HARMLESS_PREFIX, "").trim())
    .filter(Boolean);
}

function nonNegativeInt(value) {
  return Number.isInteger(value) && value >= 0 ? value : 0;
}

function readState() {
  try {
    const parsed = JSON.parse(readFileSync(STATE_PATH, "utf8"));
    return {
      count: nonNegativeInt(parsed?.count),
      // Count value at which nudge() last actually emitted a reminder, so a
      // session that ignores it isn't re-reminded on every single following
      // prompt -- only once it has accumulated another threshold's worth of
      // hand-rolled calls past the last reminder.
      lastNudgedAt: nonNegativeInt(parsed?.lastNudgedAt),
    };
  } catch {
    return { count: 0, lastNudgedAt: 0 };
  }
}

function writeState(state) {
  try {
    writeFileSync(STATE_PATH, JSON.stringify(state));
  } catch {
    // Best-effort: a failed write just means the next check starts from
    // whatever was last persisted (or zero). Never throw from here.
  }
}

function readStdinJSON() {
  try {
    return JSON.parse(readFileSync(0, "utf8"));
  } catch {
    return null;
  }
}

function track() {
  const command = readStdinJSON()?.tool_input?.command;
  if (typeof command !== "string") return;
  const fragments = commandFragments(command);
  const state = readState();
  if (fragments.some((fragment) => EPIC_STATUS_RUN.test(fragment))) {
    writeState({ count: 0, lastNudgedAt: 0 });
  } else if (fragments.some((fragment) => RECONCILE_CALL.test(fragment))) {
    writeState({ ...state, count: state.count + 1 });
  }
}

function nudge() {
  const state = readState();
  if (
    state.count < NUDGE_THRESHOLD ||
    state.count < state.lastNudgedAt + NUDGE_THRESHOLD
  ) {
    return;
  }
  process.stdout.write(
    `${state.count} hand-rolled \`gh issue/pr view\`-style calls since the ` +
      "last `scripts/epic-status.sh` run (issue #613). If you're " +
      "tracking an epic, run epic-status.sh instead of continuing to " +
      "poll by hand.",
  );
  writeState({ ...state, lastNudgedAt: state.count });
}

const mode = process.argv[2];
try {
  if (mode === "track") track();
  else if (mode === "nudge") nudge();
} catch {
  // Fail open: this hook only ever adds a reminder, never blocks or throws.
}

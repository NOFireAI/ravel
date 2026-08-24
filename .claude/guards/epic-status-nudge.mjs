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
// epic-status.sh.
const RECONCILE_CALL = /\bgh\s+(issue|pr)\s+(view|list)\b/;
const EPIC_STATUS_RUN = /\bscripts\/epic-status\.sh\b/;

function readState() {
  try {
    const parsed = JSON.parse(readFileSync(STATE_PATH, "utf8"));
    return typeof parsed?.count === "number" ? parsed : { count: 0 };
  } catch {
    return { count: 0 };
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
  const state = readState();
  if (EPIC_STATUS_RUN.test(command)) {
    state.count = 0;
    writeState(state);
  } else if (RECONCILE_CALL.test(command)) {
    state.count = (state.count ?? 0) + 1;
    writeState(state);
  }
}

function nudge() {
  const { count } = readState();
  if (count >= NUDGE_THRESHOLD) {
    process.stdout.write(
      `${count} hand-rolled \`gh issue/pr view\`-style calls since the ` +
        "last `scripts/epic-status.sh` run (issue #613). If you're " +
        "tracking an epic, run epic-status.sh instead of continuing to " +
        "poll by hand.",
    );
  }
}

const mode = process.argv[2];
try {
  if (mode === "track") track();
  else if (mode === "nudge") nudge();
} catch {
  // Fail open: this hook only ever adds a reminder, never blocks or throws.
}

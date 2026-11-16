#!/usr/bin/env node
// `ravel-agents-scaffold <dir>`: runs src/scaffold/main.ts. The package ships
// TypeScript sources, so this re-runs node with type transformation and the
// hook that maps the sources' `.js` import spellings to their `.ts` files.
/* global process, console */
import { spawnSync } from "node:child_process";
import { fileURLToPath, URL } from "node:url";

const hooks = new URL("../../../examples/ts-hooks.js", import.meta.url);
const main = fileURLToPath(new URL("../src/scaffold/main.ts", import.meta.url));
const run = spawnSync(
  process.execPath,
  [
    "--experimental-transform-types",
    "--disable-warning=ExperimentalWarning",
    "--import",
    hooks.href,
    main,
    ...process.argv.slice(2),
  ],
  { stdio: "inherit" },
);
if (run.error !== undefined) {
  console.error(`ravel-agents-scaffold: ${run.error.message}`);
  process.exit(1);
}
process.exit(run.status ?? 1);

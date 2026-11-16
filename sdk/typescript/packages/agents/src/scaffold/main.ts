/**
 * `ravel-agents-scaffold <dir>`: write the starter project into `dir`.
 *
 * Exit codes: 0 written, 1 refused (the error's code and message on one line
 * to stderr; nothing was written), 64 usage.
 */
import { scaffold, ScaffoldError } from "./index.js";

const args = process.argv.slice(2);
const dir = args[0];
if (args.length !== 1 || dir === undefined || dir === "" || dir.startsWith("-")) {
  console.error("usage: ravel-agents-scaffold <dir>");
  process.exit(64);
}

try {
  const result = await scaffold(dir);
  console.log(`wrote ${String(result.files.length)} files to ${result.dir}:`);
  for (const file of result.files) console.log(`  ${file}`);
  console.log("");
  console.log(`next: cd ${result.dir} && npm install && npm test`);
} catch (error) {
  if (!(error instanceof ScaffoldError)) throw error;
  console.error(`${error.code}: ${error.message}`);
  process.exitCode = 1;
}

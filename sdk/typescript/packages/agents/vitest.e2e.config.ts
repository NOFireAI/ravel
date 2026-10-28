import { defineConfig } from "vitest/config";
import { plan } from "./e2e/server.js";

const target = plan();
if (target.missing !== undefined) console.warn(`e2e skipped: ${target.missing}`);

// Each recovery waits out a 30 s lease and its 10 s grace before the second
// worker can take the turn over. The verbose reporter names each skipped test,
// whose suite title carries the reason.
export default defineConfig({
  test: {
    include: ["e2e/**/*.test.ts"],
    reporters: ["verbose"],
    testTimeout: 300_000,
    hookTimeout: 120_000,
  },
});

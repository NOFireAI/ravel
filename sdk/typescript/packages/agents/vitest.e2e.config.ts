import { configDefaults, defineConfig } from "vitest/config";
import { decide, plan } from "./e2e/server.js";

const decision = decide(plan());
if (decision.kind === "skip") console.warn(`e2e skipped: ${decision.reason}`);
if (decision.kind === "fail") console.error(`e2e required by RAVEL_E2E_REQUIRE=1 but cannot run: ${decision.reason}`);

// Each recovery waits out a 30 s lease and its 10 s grace before the second
// worker can take the turn over. The verbose reporter names each skipped test,
// whose suite title carries the reason. `*.unit.test.ts` files need no server
// and run under `npm test` instead. The e2e files run one at a time: each
// starts its own server, and under RAVEL_E2E_STORE=rustfs they share one
// container, bucket and tenant, which the first suite to finish would stop.
export default defineConfig({
  test: {
    include: ["e2e/**/*.test.ts"],
    fileParallelism: false,
    exclude: [...configDefaults.exclude, "e2e/**/*.unit.test.ts"],
    reporters: ["verbose"],
    testTimeout: 300_000,
    hookTimeout: 120_000,
  },
});

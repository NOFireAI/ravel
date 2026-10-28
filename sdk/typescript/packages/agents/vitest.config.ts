import { defineConfig } from "vitest/config";

// The e2e suite needs a built ravel-server; `npm run test:e2e` runs it. Its
// `*.unit.test.ts` files need no server and run here.
export default defineConfig({
  test: { include: ["src/**/*.test.ts", "e2e/**/*.unit.test.ts"] },
});

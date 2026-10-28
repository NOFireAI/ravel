import { defineConfig } from "vitest/config";

// Each recovery waits out a 30 s lease and its 10 s grace before the second
// worker can take the turn over.
export default defineConfig({
  test: {
    include: ["e2e/**/*.test.ts"],
    testTimeout: 300_000,
    hookTimeout: 120_000,
  },
});

import { configDefaults, defineConfig } from "vitest/config";

// The e2e suite needs a built ravel-server; `npm run test:e2e` runs it. Its
// `*.unit.test.ts` files need no server and run here. The scaffold template's
// own suite runs inside a generated project (`src/scaffold/index.test.ts`).
export default defineConfig({
  test: {
    include: ["src/**/*.test.ts", "e2e/**/*.unit.test.ts"],
    exclude: [...configDefaults.exclude, "src/scaffold/template/**"],
  },
});

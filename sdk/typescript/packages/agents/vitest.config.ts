import { configDefaults, defineConfig } from "vitest/config";

// The e2e suite needs a built ravel-server; `npm run test:e2e` runs it.
export default defineConfig({
  test: { exclude: [...configDefaults.exclude, "e2e/**"] },
});

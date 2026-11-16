// Lets node run the examples' TypeScript sources directly: a relative `.js`
// import from a `.ts` file (the NodeNext spelling the compiler checks) loads
// the `.ts` file beside it when there is one.
import { existsSync } from "node:fs";
import { registerHooks } from "node:module";
import { fileURLToPath, URL } from "node:url";

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier.startsWith(".") && specifier.endsWith(".js") && context.parentURL?.endsWith(".ts")) {
      const source = new URL(`${specifier.slice(0, -3)}.ts`, context.parentURL);
      if (existsSync(fileURLToPath(source))) return nextResolve(source.href, context);
    }
    return nextResolve(specifier, context);
  },
});

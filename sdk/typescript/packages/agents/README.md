# @ravel/agents

TypeScript SDK for Ravel Agents, the durable agent runtime described in
ADR-3331 (decision 11). The package name is provisional and the package is
never published (`"private": true` in `package.json`).

## Status: skeleton, no runtime

This package declares the public API and contains no runtime code yet.
Nothing in Ravel calls it, and no CI job runs it yet.

- Every public function throws `NotImplementedError`, whose `task` field
  is `T12a`, the epic task that fills it in. The test
  `src/not-implemented.test.ts` checks this for every exported function.
- Task T2 adds the `sdk` CI job. Until then, run the checks by hand with
  `make test-sdk` and `make lint-sdk` from the repository root.

## Entry points

| Import | Contents |
|---|---|
| `@ravel/agents` | The typed errors and shared types |
| `@ravel/agents/client` | `createClient`: send messages to agents and follow operations |
| `@ravel/agents/worker` | `defineAgent`, `message`, `serveAgents` |
| `@ravel/agents/tools` | `defineTool` |
| `@ravel/agents/testing` | `createTestHarness`: in-process agents for tests |

The client entry imports no server-only module: nothing under `worker/`,
`tools/` or `testing/`, and none of `node:child_process`, `node:fs` or
`node:net`. The test `client entry imports no server-only module` in
`src/client/index.test.ts` walks the client entry's import graph and
fails on any of them.

A handler that reads agent state is declared through the `messages`
builder, so its context has the agent's state type:

```ts
const counter = defineAgent({
  name: "counter",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    increment: message({
      payload: z.object({ by: z.number() }),
      result: z.number(),
      handler: async (ctx, payload) => {
        const total = ctx.state.total + payload.by;
        ctx.setState({ total });
        return total;
      },
    }),
  }),
});
```

## Errors

The eleven errors of ADR-3331 decision 11 subclass `RavelAgentsError`. Each
error carries a `code` equal to its class name, the safe ids `ids` (agent,
incarnation, request, operation, turn, step, effect, epoch; never a
payload), and a `retry` hint: `never`, `backoff`, `resolve` or `reconcile`.
`src/errors.test.ts` asserts the set of classes and each class's code and
hint.

## Checks

Run from `sdk/typescript/` after `npm ci`:

- `npm run lint`: ESLint with the `typescript-eslint` strict type-checked
  rules.
- `npm run typecheck`: `tsc` in strict mode over `src/` and `typetests/`.
- `npm test`: the vitest suites under `src/`.

`typetests/` holds compile-time negative tests. Each `// @ts-expect-error`
case (an invalid handler signature, an invalid payload type, and others)
must fail to compile. If a type change lets one compile, tsc reports the
unused directive as error TS2578, and `npm run typecheck` fails.

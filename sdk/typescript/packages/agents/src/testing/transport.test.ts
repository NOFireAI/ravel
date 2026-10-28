import { describe, expect, it } from "vitest";
import { z } from "zod";
import { createClient } from "../client/index.js";
import { OwnershipLost } from "../errors.js";
import { turnRandomSeed } from "../identity.js";
import { defineAgent, handlerDigest } from "../worker/index.js";
import { heldLease } from "../wire/index.js";
import { FakeCoordinator } from "./index.js";

const counter = defineAgent({
  name: "counter",
  version: "1",
  state: z.object({ total: z.number() }),
  initialState: { total: 0 },
  messages: (message) => ({
    add: message({
      payload: z.object({ by: z.number() }),
      result: z.number(),
      handler: (ctx, payload) => Promise.resolve(ctx.state.total + payload.by),
    }),
  }),
});

const START = 1_767_225_600_000_000_000n;

describe("fake coordinator worker transport", () => {
  it("leases carry the recorded time and seed, and heartbeats extend a held lease only", async () => {
    const coordinator = new FakeCoordinator({ agents: [counter], startUnixNs: START });
    const client = createClient({ url: "http://fake.invalid", token: "t", fetch: coordinator.fetch });
    const handlers = [{ handler: "add", version: "1", digest: handlerDigest(counter, "add") }];

    const { worker_id } = await coordinator.register({ handlers });
    expect(worker_id).toBe("w-0000000000000001");
    expect((await coordinator.register({ handlers })).worker_id).toBe("w-0000000000000002");

    await client.agent<typeof counter>("counter", "c-1").send("add", { by: 1 });
    coordinator.advanceTime(5_000);
    const polled = await coordinator.poll({ worker_id, handlers, wait_ms: 0 }, new AbortController().signal);
    const a = polled?.assignment;
    if (a === undefined) throw new Error("the fake leased no turn");
    expect(a.lease.recorded_unix_ns).toBe(START + 5_000_000_000n);
    expect(a.lease.random_seed).toBe(turnRandomSeed(a.lease.agent_id, 1, 1));

    const lease = heldLease(a.lease);
    const first = await coordinator.heartbeat({ lease });
    expect(first.lease_expires_unix_ns).toBe(START + 35_000_000_000n);
    coordinator.advanceTime(1_000);
    const second = await coordinator.heartbeat({ lease });
    expect(second.lease_expires_unix_ns).toBe(first.lease_expires_unix_ns + 1_000_000_000n);

    coordinator.revokeLeases();
    await expect(coordinator.heartbeat({ lease })).rejects.toBeInstanceOf(OwnershipLost);
    coordinator.close();
  });
});

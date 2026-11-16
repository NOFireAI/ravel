/**
 * Serve the order agent against a running server and check out one line:
 * `npm start -- <sku> <qty>`.
 *
 * Reads RAVEL_URL (default http://127.0.0.1:4318), RAVEL_AGENTS_TOKEN (a
 * credential with the `agents` capability) and RAVEL_WORKER_TOKEN (one with
 * `agents_worker`). The tests never run this file.
 */
import { randomUUID } from "node:crypto";
import { RavelAgentsError } from "@ravel/agents";
import { createClient } from "@ravel/agents/client";
import { createHttpTransport, serveAgents } from "@ravel/agents/worker";
import { createOrderAgent } from "./agent.js";
import { orderClient } from "./client.js";
import { PriceList } from "./fixture.js";

function oneLine(error: unknown): string {
  const text = error instanceof RavelAgentsError ? `${error.code}: ${error.message}` : String(error);
  return text.replace(/\s*\n\s*/g, " ");
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    console.error(`${name} is not set`);
    process.exit(64);
  }
  return value;
}

const url = process.env.RAVEL_URL ?? "http://127.0.0.1:4318";
const clientToken = required("RAVEL_AGENTS_TOKEN");
const workerToken = required("RAVEL_WORKER_TOKEN");
const sku = process.argv[2] ?? "A-1";
const qty = Number(process.argv[3] ?? "2");

const prices = new PriceList();
const { order, tools } = createOrderAgent(prices);
const workerStopped = (error: unknown): never => {
  console.error(`worker stopped: ${oneLine(error)}`);
  process.exit(1);
};
const worker = await serveAgents({
  transport: createHttpTransport({ url, token: workerToken }),
  agents: [order],
  tools,
}).catch(workerStopped);
const stopped = worker.done.then(() => undefined, workerStopped);
try {
  const orders = orderClient(createClient({ url, token: clientToken }), `order-${randomUUID()}`);
  const handle = await orders.send("checkout", { sku, qty });
  const result = await handle.result({ deadlineMs: 60_000 });
  console.log(`operation ${handle.operationId ?? "unknown"}`);
  console.log(`line      ${result.lineCents === null ? `${sku} is not on the price list` : `${String(result.lineCents)} cents`}`);
  console.log(`total     ${String(result.totalCents)} cents`);
} catch (error) {
  console.error(oneLine(error));
  process.exitCode = 1;
} finally {
  await worker.close();
}
await stopped;

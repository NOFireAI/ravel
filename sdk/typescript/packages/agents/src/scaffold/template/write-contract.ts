/**
 * Rewrite `contract.ts` from the order agent's handler schemas:
 * `npm run contract`. Run it after changing a payload or result schema; the
 * test `contract.ts is the generator's output for the agent` fails until you
 * do.
 */
import { writeFileSync } from "node:fs";
import { generateContract } from "@ravel/agents/contract";
import { createOrderAgent } from "./agent.js";
import { PriceList } from "./fixture.js";

writeFileSync(new URL("./contract.ts", import.meta.url), generateContract(createOrderAgent(new PriceList()).order));

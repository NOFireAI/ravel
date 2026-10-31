/**
 * The assistant's local tool fixture: a fixed knowledge base and a ticket
 * desk, both in memory. Nothing here touches the network, reads a clock or
 * draws a random number, so the same question always gets the same answer
 * and a test can count every call. Given a counters file, every call is also
 * appended to it, so runs in several processes are counted in one place.
 */
import { appendFileSync, existsSync, readFileSync } from "node:fs";
import type { ToolContext } from "@ravel/agents/tools";

export interface Article {
  readonly id: string;
  readonly keywords: readonly string[];
  readonly text: string;
}

export const ARTICLES: readonly Article[] = [
  {
    id: "kb-1",
    keywords: ["durable", "durability", "storage"],
    text: "Object storage is the only durable component.",
  },
  {
    id: "kb-2",
    keywords: ["crash", "kill", "restart", "recover"],
    text: "A worker that dies mid-turn is replaced and the turn is re-run.",
  },
  {
    id: "kb-3",
    keywords: ["step", "tool", "recover", "replay"],
    text: "A step whose result is journaled is replayed, never run again.",
  },
];

/** One run of a tool: which tool, under which effect handle and attempt. */
export interface ToolCall {
  readonly tool: string;
  readonly effectId: string;
  readonly attempt: number;
}

/** One line of a counters file: a tool run and what its side effect returned. */
export interface CountedCall extends ToolCall {
  readonly output: unknown;
}

/**
 * Hold the run of `tool` at `attempt` after its side effect happened and
 * before it returns: `onHeld` is called once, and the tool returns when
 * `release` resolves. A release that never resolves leaves the step open
 * for a process kill to land in.
 */
export interface Hold {
  readonly tool: string;
  readonly attempt: number;
  readonly release: Promise<void>;
  readonly onHeld?: (call: ToolCall) => void;
}

/** The calls appended to `counters`, in order; none when the file does not exist. */
export function readCounters(counters: string): CountedCall[] {
  if (!existsSync(counters)) return [];
  return readFileSync(counters, "utf8")
    .split("\n")
    .filter((line) => line !== "")
    .map((line) => JSON.parse(line) as CountedCall);
}

export class AssistantFixture {
  /** Every tool run in this process, in order, including runs that were held. */
  readonly calls: ToolCall[] = [];
  /** Tickets the desk filed, in order. */
  readonly tickets: string[] = [];
  private holds: Hold[] = [];

  /** `counters`, when set, is a file every call is appended to as one JSON line. */
  constructor(private readonly counters?: string) {}

  /** Arm `hold`; it applies to the first matching run. */
  holdAt(hold: Hold): void {
    this.holds.push(hold);
  }

  /** How many times `tool` ran. */
  count(tool: string): number {
    return this.calls.filter((call) => call.tool === tool).length;
  }

  /** Record the run, perform `effect`, count it, then wait out a matching hold and return. */
  run<T>(tool: string, ctx: ToolContext, effect: () => T): Promise<T> {
    const call: ToolCall = { tool, effectId: ctx.effectId, attempt: ctx.attempt };
    this.calls.push(call);
    const value = effect();
    if (this.counters !== undefined) {
      appendFileSync(this.counters, `${JSON.stringify({ ...call, output: value })}\n`);
    }
    const index = this.holds.findIndex((h) => h.tool === tool && h.attempt === ctx.attempt);
    const hold = index === -1 ? undefined : this.holds.splice(index, 1)[0];
    if (hold === undefined) return Promise.resolve(value);
    hold.onHeld?.(call);
    return hold.release.then(() => value);
  }

  /** Article ids whose keywords appear in `query`, in id order. */
  search(query: string): string[] {
    const words = new Set(query.toLowerCase().match(/[a-z]+/g) ?? []);
    return ARTICLES.filter((a) => a.keywords.some((k) => words.has(k))).map((a) => a.id);
  }

  /** The text of `articles`, joined, or a fixed reply when none matched. */
  draft(articles: readonly string[]): string {
    const texts = ARTICLES.filter((a) => articles.includes(a.id)).map((a) => a.text);
    return texts.length === 0 ? "I found nothing on that." : texts.join(" ");
  }

  /** File a ticket; the desk numbers them from T-1001. */
  file(): string {
    const ticket = `T-${String(1001 + this.tickets.length)}`;
    this.tickets.push(ticket);
    return ticket;
  }
}

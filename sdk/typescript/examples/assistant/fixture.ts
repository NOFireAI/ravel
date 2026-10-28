/**
 * The assistant's local tool fixture: a fixed knowledge base and a ticket
 * desk, both in memory. Nothing here touches the network, reads a clock or
 * draws a random number, so the same question always gets the same answer
 * and a test can count every call.
 */
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

/**
 * Stop the run of `tool` at `attempt` after its side effect happened and
 * before it returns, as a process killed at that instant would. `fire` is
 * called once and returns the error the tool throws.
 */
export interface Interrupt {
  readonly tool: string;
  readonly attempt: number;
  readonly fire: () => Error;
}

export class AssistantFixture {
  /** Every tool run, in order, including runs that were interrupted. */
  readonly calls: ToolCall[] = [];
  /** Tickets the desk filed, in order. */
  readonly tickets: string[] = [];
  private interrupts: Interrupt[] = [];

  /** Arm `interrupt`; it fires on the first matching run. */
  interruptAt(interrupt: Interrupt): void {
    this.interrupts.push(interrupt);
  }

  /** How many times `tool` ran. */
  count(tool: string): number {
    return this.calls.filter((call) => call.tool === tool).length;
  }

  /** Record the run, perform `effect`, then fire a matching interrupt or return. */
  run<T>(tool: string, ctx: ToolContext, effect: () => T): Promise<T> {
    this.calls.push({ tool, effectId: ctx.effectId, attempt: ctx.attempt });
    const value = effect();
    const index = this.interrupts.findIndex((i) => i.tool === tool && i.attempt === ctx.attempt);
    const interrupt = index === -1 ? undefined : this.interrupts.splice(index, 1)[0];
    if (interrupt !== undefined) return Promise.reject(interrupt.fire());
    return Promise.resolve(value);
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

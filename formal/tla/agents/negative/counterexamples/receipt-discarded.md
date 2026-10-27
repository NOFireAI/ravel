# Counterexample: turn completion drops the input's receipt

Config: `negative/receipt-discarded.cfg` (`ReceiptDiscarded = TRUE`).
Expected: `RequestIdentityStable` violated, TLC exit 12.

Observed TLC line:

    Error: Invariant RequestIdentityStable is violated.

Trace (8 states, depth 8):

    State 1: <Initial predicate>
    State 2: <CreateHead in AgentProtocol>
    State 3: <ReadHead in AgentProtocol>
    State 4: <CasHead in AgentProtocol>
    State 5: <Takeover in AgentProtocol>
    State 6: <BeginTurn in AgentProtocol>
    State 7: <PutTurnOutput in AgentProtocol>
    State 8: <BrokenReceiptDiscarded in MCAgentProtocol>

## What the switch breaks

Receipts live in HEAD for the whole incarnation, so a retried send finds its
original handle after the input has been processed. The switch removes the
receipt in the same CAS that completes the turn on its input.

## Why it is a real bug if it ever held

`RequestIdentityStable`'s transition clause says no write within an
incarnation drops a receipt. The completion in state 8 does. After it, a
client retrying the same request id (a lost response is enough to cause one)
finds no receipt and enqueues the message a second time, so one logical send
runs two turns. The correct model (switch FALSE) keeps the receipt.

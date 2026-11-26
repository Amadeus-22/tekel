# KleverChain blocks

Set `TEKEL_KLEVER_NETWORK=mainnet` (or `testnet`) to follow every block of
that KleverChain network through its public node's WebSocket (`/subscribe`,
`blocks`). The stream shows up as `klever:mainnet` (or `klever:testnet`) with `"kind": "blocks"`: `trades` counts
blocks and `last_trade_id` is the last block nonce. A nonce jump while
connected is a `sequence_gap` (the node drops events when its queues are
full: the block is still on chain, only this subscription missed its event), a dropped connection a `disconnect`, no block for too long a
`stale_stream`. Blocks have no price: no `price_spike`, no LEAN files.

A block comes every 4 s, so a tighter stale threshold suits it:
`TEKEL_STALE_AFTER_OVERRIDES=klever:mainnet=20s`.

## Clock skew

A block's `timestamp` is the start of its round in whole seconds, and the node
publishes the block once it is final, so blocks reach Tekel about 2 to 2.5 s
after their timestamp. On mainnet, 8,116 blocks between 2026-11-21 and
2026-11-24 crossed the default 2 s threshold; 92% of them by less than 0.5 s,
none early. That is the chain's finality delay, not a wrong clock. Allow one
block interval: `TEKEL_CLOCK_SKEW_OVERRIDES=klever:mainnet=4s`. A block later
than that is still flagged.

The public nodes are `wss://node.mainnet.klever.org/subscribe` and
`wss://node.testnet.klever.org/subscribe`; set `TEKEL_KLEVER_URL` to use your
own node instead. A node that serves another chain than the configured network
stops Tekel with an error. Design and sources:
[ADR 0007](adr/0007-klever-block-feed.md).

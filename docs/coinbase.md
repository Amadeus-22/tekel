# Coinbase trades

Coinbase is on by default with `BTC-USD,ETH-USD`; set `TEKEL_COINBASE_SYMBOLS` to
other product IDs, or to an empty value to turn it off. Tekel connects to the public
Exchange feed (`wss://ws-feed.exchange.coinbase.com`), with no account or key, and
subscribes to two channels for every product:

- `matches`: one message per trade, with a `trade_id` that increases by one per
  product. Coinbase's own documentation says match messages can be dropped and
  points to the trade ID to notice it; that is the `sequence_gap` check Tekel runs.
- `heartbeat`: one message per product every second, which keeps a quiet product's
  connection inside the 15 s read timeout.

Details that matter:

- `side` in a match is the **maker's** side; Tekel stores the taker's side, the
  opposite, like the other exchanges.
- Right after subscribing, Coinbase sends one `last_match` per product: a trade from
  before the connection. Tekel skips it; after a reconnect the `disconnect` incident
  already records the window.
- An unknown product is answered with `{"type":"error","reason":"X is not a valid
  product"}`; Tekel stops with that error instead of retrying.
- The LEAN market is `coinbase` (`Market.Coinbase`), so minute bars go to
  `crypto/coinbase/minute/<ticker>/`.

The adapter's tests replay messages recorded from the live feed on 2026-11-24,
[`internal/adapter/coinbase/testdata/feed.jsonl`](../internal/adapter/coinbase/testdata/feed.jsonl).

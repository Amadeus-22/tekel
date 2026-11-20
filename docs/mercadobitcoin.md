# Mercado Bitcoin (Brazil)

> **Status, 2026-11-20: blocked at the provider's edge.** Mercado Bitcoin's
> websocket (`wss://ws.mercadobitcoin.net/ws`), its REST API and even its API
> documentation answer HTTP 403 from Cloudflare to automated clients. We saw it
> from a residential connection in Brazil and from a cloud network, with and
> without Tekel. Tekel logs `dial wss://ws.mercadobitcoin.net/ws: websocket: bad
> handshake (HTTP 403)` and keeps retrying with backoff; nothing in Tekel can or
> should get around the block. The feed is off by default, so this only affects
> you if you set `TEKEL_MERCADOBITCOIN_SYMBOLS`. If it starts answering again,
> please open an issue so this note can go.

Set `TEKEL_MERCADOBITCOIN_SYMBOLS=BTC-BRL,USDT-BRL` (Mercado Bitcoin's
`BASE-QUOTE` spelling). Bars go to
`crypto/mercadobitcoin/minute/btcbrl/<yyyymmdd>_trade.zip`.

**LEAN has no Mercado Bitcoin market** — there is no constant for it in
[`Common/Market.cs`](https://github.com/QuantConnect/Lean/blob/master/Common/Market.cs).
To backtest on this data you must register a custom market yourself:
`Market.Add("mercadobitcoin", <an unused id below 1000>)` before creating the
symbol, a `"Crypto-mercadobitcoin-[*]"` entry (24/7, UTC) in
`Data/market-hours/market-hours-database.json`, and one
`mercadobitcoin,<ticker>,crypto,...` row per pair in
`Data/symbol-properties/symbol-properties-database.csv`. Tekel does not
ship these; see [ADR 0006](adr/0006-mercado-bitcoin-adapter.md).

BRL markets can be quiet for minutes, so give them per-stream stale thresholds,
e.g. `TEKEL_STALE_AFTER_OVERRIDES=mercadobitcoin:ETH-BRL=10m`. Mercado
Bitcoin stamps trades in whole seconds, so its `tekel_feed_lag_seconds`
reads up to one second high.

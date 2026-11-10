# Mercado Bitcoin (Brazil)

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

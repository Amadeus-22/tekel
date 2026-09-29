# 0006 — Mercado Bitcoin adapter over its public WebSocket

Status: accepted · 2026-09-29

## Context

Mercado Bitcoin is Brazil's largest crypto exchange (BRL-quoted markets). LEAN
has no Brazilian crypto market, so a Brazil-focused quant has neither data nor
a feed. The adapter must follow the Binance/Kraken pattern (shared `wsconn`
reconnect loop, exact decimals, sequence checks) and must not pretend LEAN
supports the market upstream.

## Sources checked (2026-09-29)

1. WebSocket API v0.0.4, <https://ws.mercadobitcoin.net/docs/v0/> (docsify
   pages `README.md`, `api/GeneralMessages.md`, `api/PublicMessages.md`,
   `api/Errors.md`, `CHANGELOG.md`):
   - endpoint `wss://ws.mercadobitcoin.net/ws`; "A `User-Agent` header
     **SHOULD** be sent"; "If a connection is made and no message is sent in
     5 seconds, the current connection is closed"; "Do a reconnection if a
     connection is closed"; "Timestamps should not be considered unique";
   - `{"type":"ping"}` → `{"type":"pong"}`;
   - subscribe `{"type":"subscribe","subscription":{"name":"trade","id":"BRLBTC"}}`,
     acknowledged with `{"id":"BRLBTC","name":"trade"}`;
   - trade `{"type":"trade","id":"BRLBTC","ts":<ns send time>,"data":{"tid","date"
     (Unix s),"type" (buy|sell),"price" (float),"amount" (float)}}`;
   - errors `{"type":"error","message":...}`: unknown message type, invalid
     JSON, already subscribed, not subscribed.
2. REST API v4, <https://api.mercadobitcoin.net/api/v4/docs> (`docs/swagger.yaml`):
   `GET /{symbol}/trades` (`BTC-BRL` spelling; `tid`, `since`, `from`/`to`,
   `limit` ≤ 1000; price and amount as strings), **1 request/s** for that
   endpoint and **500 requests/min** in total per client.
3. Live check of production (≤ 60 s, public data only):
   - messages match the documentation, prices and amounts are JSON numbers
     (`"price":435832,"amount":0.00000222`);
   - subscribing to an unknown market returns
     `{"type":"error","message":"invalid pair to subscribe"}` (undocumented);
   - REST: the last 1000 trades of BTC-BRL and USDT-BRL have consecutive
     `tid`s, and the WebSocket delivered consecutive `tid`s (ETH-BRL
     8773601…8773604): **trade IDs are sequential per market**;
   - quiet markets go 30+ s without a trade; the server sent no heartbeat.
4. LEAN `Common/Market.cs`
   (<https://github.com/QuantConnect/Lean/blob/master/Common/Market.cs>): no
   Mercado Bitcoin or Brazilian crypto constant; `Market.Add(string market, int
   identifier)` registers a custom market (identifier < 1000).

## Decision

- **WebSocket only, no REST polling.** The WebSocket has everything the checks
  need (per-market sequential `tid`, taker side, exact numeric literals). A REST
  poller at 1 request/s per endpoint could not follow several markets and would
  add a second failure mode for no gain. REST stays the tool for the Phase 3
  backfill of silent gaps (`since=<last tid>`, `limit=1000`, one request per
  second).
- Symbols are configured in the REST spelling `BTC-BRL`; the adapter
  subscribes with the WebSocket id `BRLBTC` (quote first) and reports trades as
  `BTC-BRL`, so the LEAN ticker is `btcbrl` (base+quote, like `btcusdt`).
- Liveness: a `{"type":"ping"}` every 5 s (new `wsconn.Config.AppPing`) and a
  15 s read deadline; the `pong` reply counts as traffic. A `User-Agent` is
  sent (new `wsconn.Config.Header`).
- Any `{"type":"error"}` is permanent: every documented error means our
  requests are wrong, so reconnecting would repeat it. The process exits with
  the server's message and the configured symbols.
- `ExchangeTime` is `data.date`, whole seconds. `ts` is only the send time. The
  feed lag metric therefore reads up to 1 s high for this exchange, and
  `clock_skew` (default 2 s) has 1 s less headroom.
- Prices and amounts are decoded as `json.Number`, keeping the literal text
  (no float64), as for Kraken.
- LEAN output goes to `crypto/mercadobitcoin/minute/<ticker>/`. **LEAN does not
  know this market.** To backtest on it, the user must register it
  (`Market.Add("mercadobitcoin", <unused id < 1000>)`), add a
  `Crypto-mercadobitcoin-[*]` entry to `market-hours-database.json` (24/7, UTC)
  and `mercadobitcoin,<ticker>,crypto,...` rows to
  `symbol-properties-database.csv`. The README says so explicitly.
- Mercado Bitcoin is disabled by default (`TICKGUARD_MERCADOBITCOIN_SYMBOLS`
  empty) for the same reason.

## Consequences

- Illiquid BRL markets are often silent for longer than the 60 s default
  `stale_stream` threshold; per-stream overrides are recommended in the README.
- The 5 s idle rule is met because subscriptions go out right after the
  handshake and pings follow every 5 s.
- If Mercado Bitcoin changes its undocumented behaviour (sequential `tid`,
  numeric prices), the decoder rejects the message and counts a decode error
  rather than guessing.

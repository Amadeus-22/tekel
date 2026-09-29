# 0002 — Distinguish known gaps from silent gaps

Status: accepted · 2026-09-29

## Context

Both Binance (`t`) and Kraken v2 (`trade_id`, "a sequence number, unique per
book") give each trade a per-symbol sequence number. A jump in that number
means trades are missing. But a jump is expected after every reconnect —
Binance closes every connection at the 24-hour mark by design — and flagging
those as defects would bury the real ones.

The two cases need different responses:

| | Known gap | Silent gap |
|---|---|---|
| Cause | we were disconnected | the feed skipped data while connected |
| Window | exactly known: disconnect → reconnect | between two consecutive trades |
| Who acts | operations (network, exchange status) | data quality (backfill, distrust the day) |

## Decision

- A disconnect is recorded as one `disconnect` incident per stream, emitted on
  reconnect with `start`/`end` set to the window. If the process stops while
  disconnected, the open window is closed and recorded at shutdown.
- On (re)connect, `quality.Stream` resets its sequence baseline: the first trade
  after a reconnect never raises `sequence_gap`.
- While connected, `TradeID > last + 1` raises `sequence_gap` with the missing
  count and the exchange-time window between the two trades.
- A disconnected stream is never `stale_stream`: the silence is explained.
- The duplicate window survives reconnects, so a replay after reconnect is
  still caught as `duplicate`.
- Kraken is subscribed with `snapshot: false` so a reconnect does not replay
  recent trades as duplicates.

## Consequences

- `GET /v1/incidents?kind=sequence_gap` is the list of real data defects;
  `kind=disconnect` is the operational log. Both carry the window needed by a
  future REST backfill (roadmap phase 3).
- A trade ID lower than the last one that is outside the duplicate window is
  not flagged as a gap (the baseline never moves backwards); its time is still
  checked by `out_of_order`.

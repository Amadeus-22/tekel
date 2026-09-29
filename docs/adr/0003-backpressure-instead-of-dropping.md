# 0003 — Backpressure instead of dropping trades

Status: accepted · 2026-09-29

## Context

Feeds push trades into the pipeline through a channel. When the consumer is
slower (disk stall, SQLite lock, CPU contention), something has to give. The
usual choices are to drop (keep the socket drained, lose data) or to block
(keep all data, fall behind).

Dropping would make tickguard create exactly the defect it exists to detect —
a silent gap — and hide it from itself.

## Decision

The channel is bounded (`TICKGUARD_BUFFER`, default 4096 events). When it is
full, the feed's `Emit` call blocks, which stops the feed reading its socket.
Time spent blocked is added to `tickguard_backpressure_seconds_total{exchange}`.
Nothing is dropped between the socket and the validator.

## Consequences

- Under sustained overload, lag grows and is visible in
  `tickguard_feed_lag_seconds`, `clock_skew` incidents and the backpressure
  counter — an honest signal instead of silent loss.
- If the stall lasts long enough, the exchange may close the connection (Binance
  disconnects clients that do not answer pings within a minute). That becomes a
  `disconnect` incident with an exact window: a known gap, not a silent one.
- Sink failures (disk full, SQLite error) are logged and counted in
  `tickguard_sink_errors_total`; they do not stop validation.
- The only trades removed on purpose are exact duplicates (same trade ID within
  the duplicate window), which are recorded as incidents.

# 0007 — KleverChain block feed: block streams next to trade streams

Status: accepted · 2026-11-10

> Written when the project was named tickguard. [ADR 0008](0008-rename-to-tekel.md)
> renamed it to Tekel: `TICKGUARD_*` variables are now `TEKEL_*`, `tickguard_*`
> metrics `tekel_*`.

## Context

KleverChain blocks carry a sequential nonce (height), so the question tickguard
answers for trades — *is data missing, and did we lose it or did the source
skip it?* — applies unchanged to a chain feed: a nonce that jumps while
connected is a silent gap, a dropped connection is a known gap, no block for
too long is a stale stream. A block is not a trade, though: it has no price,
so `price_spike` and LEAN bars make no sense for it.

## Sources checked (2026-11-10)

1. klever-go source (`github.com/klever-io/klever-go`, branch `develop`):
   - `network/api/websocket/routes.go`: `GET /subscribe` upgrades to a
     WebSocket; the first message must be
     `{"addresses":[...],"subscribed_types":[...]}` within 30 s; an invalid
     type is answered with `{"error":"invalid subscription type: ..."}` and the
     connection is closed;
   - `websocket/README.md`: `blocks` is a global type ("every new block as it
     is produced", no addresses needed); push events are
     `{"type":"blocks","address":"","hash":"","data":{...}}`; the server pings;
   - `websocket/config.go`: ping every 15 s, pong wait 30 s;
   - `indexer/dataParser.go`: `data.timestamp` is the header's Unix seconds
     × 1000; `indexer/events.go` `trySendEvent` and `websocket/client.go`
     **drop** events when the node's event queue or the client's output
     buffer is full — exactly what a silent gap looks like from outside;
   - `network/api/block/routes.go`: REST `GET /block/by-nonce/:nonce` and
     `/block/by-hash/:hash` (not used, see below).
2. Public nodes, read-only:
   - `wss://node.testnet.klever.org/subscribe` streams blocks
     (`chainID` `"109"`), one every ~4 s, received ~0.5 s after the block
     timestamp; three were recorded into `internal/adapter/klever/testdata/`;
   - `wss://node.mainnet.klever.org/subscribe` answers **404**: the public
     mainnet node does not expose the WebSocket API; its REST API works
     (`/node/overview`: `chainID` `"108"`, `slotDuration` 4000 ms;
     `/block/by-nonce/N`);
   - slots without a block exist (mainnet nonce 34,340,812 at slot
     34,412,617), so time between blocks is a multiple of 4 s.

## Decision

- **One capability, declared once.** `app.Feed` gains
  `Kind() domain.StreamKind` (`trades` or `blocks`). The pipeline reads it in
  one place, when it registers a stream: a block stream gets
  `quality.NewBlockStream`, whose rules are the trade rules over the nonce
  minus the price check. Blocks travel as `domain.Block` in an `EventBlock`;
  the pipeline routes them to incidents, status and metrics only, so they can
  never reach the bar aggregator or the LEAN writer. No exchange-name checks
  anywhere.
- **Same contracts, reused.** A block stream appears in `/v1/streams` with
  `exchange: "klever"`, `symbol: <network>` and the new `kind` field;
  `trades` counts blocks and `last_trade_id` is the last nonce. Incidents keep
  their schema (`trade_id` holds the nonce), metrics keep their names
  (`tickguard_trades_total{exchange="klever"}` counts blocks). A separate set
  of fields or metrics for blocks would double every consumer for no new
  information.
- **WebSocket push, not REST polling.** The adapter runs on the shared
  `wsconn` loop (dial timeout, read deadline, jittered backoff, permanent
  errors) like every other feed. Polling would need a second retry loop and
  would hide the node's own drops: a poller fetches the missed nonces and
  never sees a gap. With push, a nonce jump while connected is the node's
  drop — a real silent gap.
- **Configuration.** `TICKGUARD_KLEVER_NETWORK` (`mainnet` or `testnet`)
  turns the feed on and is the stream's symbol; empty (the default) is off.
  `TICKGUARD_KLEVER_URL` defaults to the public testnet node when the network
  is `testnet`; for `mainnet` it is required, because no public mainnet node
  serves `/subscribe`.
- **Wrong chain is permanent.** Each block carries `chainID`; a block whose
  chain is not the configured network's (`108` mainnet, `109` testnet) stops
  the process instead of being recorded under the wrong name. A node
  `{"error":...}` is permanent too, as for Kraken and Mercado Bitcoin.
- **On reconnect**, as for the trade feeds (ADR 0002), the window is recorded
  as a `disconnect` incident and the sequence baseline restarts; the missed
  blocks are not fetched. Backfill through `GET /block/by-nonce/N` belongs to
  the roadmap's backfill phase.
- **Timing.** One block per 4 s slot (0.25 blocks/s), so the default
  `stale_stream` threshold (60 s, 15 slots) works; `klever:<network>=20s`
  (5 slots) is a tighter override. The read deadline is 30 s (server pings
  every 15 s). Block timestamps are whole seconds, so lag reads up to 1 s high;
  the observed lag (~0.5 s) leaves room under the 2 s `clock_skew` default.
- **Message size.** A block lists every transaction hash, so the read limit is
  raised to 8 MiB for this feed.

## Consequences

- Mainnet monitoring needs an operator-run node (or a provider) that exposes
  `/subscribe`; the README says so. The testnet works out of the box.
- `trades`/`trade_id` naming is slightly off for blocks; `kind` tells the
  reader which meaning applies.
- Forks (a different hash at a known nonce, or a `parentHash` that does not
  match) are not checked; they would be a new incident kind.

// Package coinbase is the Coinbase Exchange trade feed: one WebSocket
// connection subscribed to the "matches" and "heartbeat" channels for every
// configured product, re-sent after every reconnect.
//
// Message formats follow Coinbase's documentation
// (https://docs.cdp.coinbase.com/exchange/websocket-feed/channels) and were
// checked against the live feed (testdata/feed.jsonl):
//   - match:     {"type":"match","trade_id":10,"sequence":50,"time":"...","product_id":"BTC-USD",
//     "size":"5.23512","price":"400.23","side":"sell",...}; side is the maker's side
//   - last_match: the same shape, sent once per product right after subscribing
//   - heartbeat: {"type":"heartbeat","last_trade_id":20,"product_id":"BTC-USD",...} every second
//   - error:     {"type":"error","message":"Failed to subscribe","reason":"X is not a valid product"}
//
// The documentation warns that match messages can be dropped and points to the
// trade ID to detect it: exactly the sequence check Tekel runs on every stream.
package coinbase

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/adapter/wsfeed"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// Exchange identifies Coinbase in stream keys, logs and metrics. It doubles as
// the LEAN market name (Market.Coinbase in LEAN's Common/Market.cs).
const Exchange = "coinbase"

// DefaultURL is the public market-data endpoint; no key is needed for matches.
const DefaultURL = "wss://ws-feed.exchange.coinbase.com"

// defaultReadTimeout: the heartbeat channel sends one message per product
// every second, so 15 s of silence means the connection is dead.
const defaultReadTimeout = 15 * time.Second

var symbolRE = regexp.MustCompile(`^[A-Z0-9]{1,12}-[A-Z0-9]{1,12}$`)

// Config configures the feed.
type Config struct {
	URL string
	// Symbols are Coinbase product IDs, e.g. "BTC-USD".
	Symbols []string
	// Conn holds timeouts and backoff; Name and URL are filled in by New.
	Conn   wsconn.Config
	Logger *slog.Logger
	// DecodeErrors is called once per malformed message; nil is allowed.
	DecodeErrors func()
}

// Feed implements app.Feed for Coinbase.
type Feed struct {
	cfg       Config
	malformed *wsconn.Malformed
}

// New validates cfg and returns a feed.
func New(cfg Config) (*Feed, error) {
	if err := wsconn.CheckSymbols(Exchange, cfg.Symbols, symbolRE, "look like BTC-USD"); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		return nil, errors.New("coinbase: logger is required")
	}
	cfg.Conn.CapReadTimeout(defaultReadTimeout)
	cfg.Conn.Bind(Exchange, cfg.URL, cfg.Logger)
	return &Feed{cfg: cfg, malformed: wsconn.NewMalformed(Exchange, cfg.Logger, cfg.DecodeErrors)}, nil
}

// Exchange implements app.Feed.
func (f *Feed) Exchange() string { return Exchange }

// Symbols implements app.Feed.
func (f *Feed) Symbols() []string { return append([]string(nil), f.cfg.Symbols...) }

// Kind implements app.Feed: the feed carries trades.
func (f *Feed) Kind() domain.StreamKind { return domain.StreamTrades }

// SubscribeRequest is the documented request:
//
//	{"type":"subscribe","product_ids":["BTC-USD"],"channels":["matches","heartbeat"]}
//
// heartbeat keeps a quiet product's connection alive within the read timeout.
type SubscribeRequest struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// NewSubscribeRequest returns the subscribe request for symbols.
func NewSubscribeRequest(symbols []string) SubscribeRequest {
	return SubscribeRequest{Type: "subscribe", ProductIDs: symbols, Channels: []string{"matches", "heartbeat"}}
}

// Run implements app.Feed.
func (f *Feed) Run(ctx context.Context, emit app.Emit) error {
	subscribe := func(_ context.Context, conn *websocket.Conn) error {
		return conn.WriteJSON(NewSubscribeRequest(f.cfg.Symbols))
	}
	return wsfeed.Run(ctx, f.cfg.Conn, emit, subscribe, f.malformed, func(msg []byte, received time.Time) ([]app.Event, error) {
		m, err := Decode(msg, received)
		if err != nil {
			return nil, err
		}
		if m.SubscribeError != "" {
			// An unknown product is a configuration error; retrying cannot fix it.
			return nil, wsconn.Permanent(fmt.Errorf("subscribe rejected: %s", m.SubscribeError))
		}
		if m.Trade == nil {
			return nil, nil
		}
		return []app.Event{{Kind: app.EventTrade, At: received, Trade: *m.Trade}}, nil
	})
}

// Message is the part of one decoded Coinbase message Tekel acts on.
type Message struct {
	// Trade is set for a "match" message.
	Trade *domain.Trade
	// SubscribeError is set when the exchange rejected the subscribe request.
	SubscribeError string
}

// message covers every type Tekel reads. price and size are strings in the
// feed, so they decode to decimals without passing through float64.
type message struct {
	Type      string `json:"type"`
	TradeID   *int64 `json:"trade_id"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	Time      string `json:"time"`
	Message   string `json:"message"`
	Reason    string `json:"reason"`
}

// Decode parses one message. Subscriptions, heartbeats and last_match decode
// to an empty Message: last_match repeats a trade from before the connection,
// and after a reconnect the disconnect incident already records the window.
func Decode(msg []byte, received time.Time) (Message, error) {
	var m message
	if err := json.Unmarshal(msg, &m); err != nil {
		return Message{}, fmt.Errorf("decode message: %w", err)
	}
	switch m.Type {
	case "match":
		t, err := m.trade(received)
		if err != nil {
			return Message{}, fmt.Errorf("decode match: %w", err)
		}
		return Message{Trade: &t}, nil
	case "error":
		return Message{SubscribeError: cmp.Or(m.Reason, m.Message, "unknown error")}, nil
	default:
		return Message{}, nil
	}
}

func (m message) trade(received time.Time) (domain.Trade, error) {
	if m.TradeID == nil {
		return domain.Trade{}, errors.New("missing trade_id")
	}
	price, err := domain.ParseDecimal(m.Price)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("price: %w", err)
	}
	qty, err := domain.ParseDecimal(m.Size)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("size: %w", err)
	}
	// side is the maker's side; the trade's side is the taker's, the opposite.
	var side domain.Side
	switch m.Side {
	case "buy":
		side = domain.SideSell
	case "sell":
		side = domain.SideBuy
	default:
		return domain.Trade{}, fmt.Errorf("side %s", strconv.Quote(m.Side))
	}
	ts, err := time.Parse(time.RFC3339Nano, m.Time)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("time %s: %w", strconv.Quote(m.Time), err)
	}
	t := domain.Trade{
		Exchange: Exchange, Symbol: m.ProductID, TradeID: *m.TradeID,
		Price: price, Qty: qty, Side: side,
		ExchangeTime: ts.UTC(), ReceivedTime: received.UTC(),
	}
	if err := t.Validate(); err != nil {
		return domain.Trade{}, err
	}
	return t, nil
}

// Package kraken is the Kraken spot trade feed over WebSocket API v2: one
// connection, one subscribe request for the "trade" channel covering every
// configured symbol, re-sent after every reconnect.
//
// Message formats follow Kraken's official documentation:
//   - Trade channel:  https://docs.kraken.com/api/docs/websocket-v2/trade
//   - Heartbeat:      https://docs.kraken.com/api/docs/websocket-v2/heartbeat
package kraken

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

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

// Exchange identifies Kraken in stream keys, logs and metrics. It doubles as
// the LEAN market name, so it must match LEAN's Common/Market.cs constant.
const Exchange = "kraken"

// DefaultURL is the public WebSocket v2 endpoint.
const DefaultURL = "wss://ws.kraken.com/v2"

// defaultReadTimeout: Kraken sends a heartbeat "approximately once every second
// in the absence of any other channel updates", so 15 s of silence means the
// connection is dead.
const defaultReadTimeout = 15 * time.Second

var symbolRE = regexp.MustCompile(`^[A-Z0-9]{1,12}/[A-Z0-9]{1,12}$`)

// Config configures the feed.
type Config struct {
	URL string
	// Symbols in Kraken v2 spelling, e.g. "BTC/USD".
	Symbols []string
	// Conn holds timeouts and backoff; Name and URL are filled in by New.
	Conn   wsconn.Config
	Logger *slog.Logger
	// DecodeErrors is called once per malformed message; nil is allowed.
	DecodeErrors func()
}

// Feed implements app.Feed for Kraken.
type Feed struct {
	cfg       Config
	malformed *wsconn.Malformed
}

// New validates cfg and returns a feed.
func New(cfg Config) (*Feed, error) {
	if len(cfg.Symbols) == 0 {
		return nil, errors.New("kraken: at least one symbol is required")
	}
	for _, s := range cfg.Symbols {
		if !symbolRE.MatchString(s) {
			return nil, fmt.Errorf("kraken: symbol %q must look like BTC/USD", s)
		}
	}
	if cfg.Logger == nil {
		return nil, errors.New("kraken: logger is required")
	}
	if cfg.Conn.ReadTimeout == 0 || cfg.Conn.ReadTimeout > defaultReadTimeout {
		cfg.Conn.ReadTimeout = defaultReadTimeout
	}
	cfg.Conn.Name = Exchange
	cfg.Conn.URL = cfg.URL
	cfg.Conn.Logger = cfg.Logger
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
//	{"method":"subscribe","params":{"channel":"trade","symbol":["MATIC/USD"],"snapshot":true}}
//
// snapshot is false: replaying the last trades after a reconnect would only
// produce duplicates, and the disconnect incident already records the window.
type SubscribeRequest struct {
	Method string          `json:"method"`
	Params SubscribeParams `json:"params"`
}

// SubscribeParams are the "params" of a SubscribeRequest.
type SubscribeParams struct {
	Channel  string   `json:"channel"`
	Symbol   []string `json:"symbol"`
	Snapshot bool     `json:"snapshot"`
}

// NewSubscribeRequest returns the trade-channel subscribe request for symbols.
func NewSubscribeRequest(symbols []string) SubscribeRequest {
	return SubscribeRequest{
		Method: "subscribe",
		Params: SubscribeParams{Channel: "trade", Symbol: symbols, Snapshot: false},
	}
}

// Run implements app.Feed.
func (f *Feed) Run(ctx context.Context, emit app.Emit) error {
	return wsconn.Run(ctx, f.cfg.Conn, wsconn.Handlers{
		Subscribe: func(_ context.Context, conn *websocket.Conn) error {
			return conn.WriteJSON(NewSubscribeRequest(f.cfg.Symbols))
		},
		Up: func(ctx context.Context, at time.Time) error {
			return emit(ctx, app.Event{Kind: app.EventConnected, Exchange: Exchange, At: at})
		},
		Down: func(ctx context.Context, at time.Time, cause error) error {
			return emit(ctx, app.Event{Kind: app.EventDisconnected, Exchange: Exchange, At: at, Err: cause})
		},
		Message: func(ctx context.Context, msg []byte, received time.Time) error {
			m, err := Decode(msg, received)
			if err != nil {
				f.malformed.Report(msg, received, err)
				return nil
			}
			if m.SubscribeError != "" {
				// A rejected symbol is a configuration error; retrying cannot fix it.
				return wsconn.Permanent(fmt.Errorf("subscribe rejected: %s", m.SubscribeError))
			}
			for _, t := range m.Trades {
				ev := app.Event{Kind: app.EventTrade, Exchange: Exchange, At: received, Trade: t}
				if err := emit(ctx, ev); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

// Message is the part of one decoded Kraken message tickguard acts on.
type Message struct {
	// Trades from a "trade" channel snapshot or update, in message order.
	Trades []domain.Trade
	// SubscribeError is set when the exchange rejected a subscribe request.
	SubscribeError string
}

// envelope covers the shapes of interest:
//
//	{"channel":"trade","type":"update","data":[{...}]}
//	{"channel":"heartbeat"}
//	{"method":"subscribe","result":{"channel":"trade","snapshot":true,"symbol":"MATIC/USD"},"success":true,...}
type envelope struct {
	Channel string          `json:"channel"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data"`
	Method  string          `json:"method"`
	Success *bool           `json:"success"`
	Error   string          `json:"error"`
	Symbol  string          `json:"symbol"`
}

// tradeData is one element of "data" in a trade message:
//
//	{"symbol":"MATIC/USD","side":"sell","price":0.5117,"qty":40.0,"ord_type":"market",
//	 "trade_id":4665906,"timestamp":"2023-09-25T07:49:37.708706Z"}
//
// Kraken documents price and qty as JSON numbers ("float"). Decoding them as
// json.Number keeps the literal text, so no precision is lost to float64.
// trade_id is "a sequence number, unique per book"; side is "the side of the
// taker order"; timestamp is RFC3339.
type tradeData struct {
	Symbol    string      `json:"symbol"`
	Side      string      `json:"side"`
	Price     json.Number `json:"price"`
	Qty       json.Number `json:"qty"`
	OrdType   string      `json:"ord_type"`
	TradeID   *int64      `json:"trade_id"`
	Timestamp string      `json:"timestamp"`
}

// Decode parses one message. Heartbeats, status and other channels decode to an
// empty Message. Trade data that fails validation is an error for the whole
// message, so a partly corrupt batch is counted rather than half-applied.
func Decode(msg []byte, received time.Time) (Message, error) {
	var env envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		return Message{}, fmt.Errorf("decode envelope: %w", err)
	}
	if env.Method == "subscribe" {
		if env.Success != nil && !*env.Success {
			return Message{SubscribeError: cmp.Or(env.Error, "unknown error") + " (symbol " + env.Symbol + ")"}, nil
		}
		return Message{}, nil
	}
	if env.Channel != "trade" {
		return Message{}, nil
	}
	if env.Type != "update" && env.Type != "snapshot" {
		return Message{}, fmt.Errorf("decode trade: unknown type %q", env.Type)
	}
	var data []tradeData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		return Message{}, fmt.Errorf("decode trade data: %w", err)
	}
	trades := make([]domain.Trade, 0, len(data))
	for i, d := range data {
		t, err := d.trade(received)
		if err != nil {
			return Message{}, fmt.Errorf("decode trade %d of %d: %w", i+1, len(data), err)
		}
		trades = append(trades, t)
	}
	return Message{Trades: trades}, nil
}

func (d tradeData) trade(received time.Time) (domain.Trade, error) {
	if d.TradeID == nil {
		return domain.Trade{}, errors.New("missing trade_id")
	}
	price, err := domain.ParseDecimal(d.Price.String())
	if err != nil {
		return domain.Trade{}, fmt.Errorf("price: %w", err)
	}
	qty, err := domain.ParseDecimal(d.Qty.String())
	if err != nil {
		return domain.Trade{}, fmt.Errorf("qty: %w", err)
	}
	side, err := domain.ParseSide(d.Side)
	if err != nil {
		return domain.Trade{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, d.Timestamp)
	if err != nil {
		return domain.Trade{}, fmt.Errorf("timestamp %s: %w", strconv.Quote(d.Timestamp), err)
	}
	t := domain.Trade{
		Exchange: Exchange, Symbol: d.Symbol, TradeID: *d.TradeID,
		Price: price, Qty: qty, Side: side,
		ExchangeTime: ts.UTC(), ReceivedTime: received.UTC(),
	}
	if err := t.Validate(); err != nil {
		return domain.Trade{}, err
	}
	return t, nil
}

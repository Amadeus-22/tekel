// Package mercadobitcoin is the Mercado Bitcoin (Brazil) public trade feed over
// its WebSocket API: one connection, one "trade" subscription per market,
// re-sent after every reconnect, and an application-level ping.
//
// Message formats follow Mercado Bitcoin's official documentation (WebSocket
// API v0.0.4, checked 2026-09-29) and a live check of the production server:
//   - Connection:   https://ws.mercadobitcoin.net/docs/v0/#/ (wss://ws.mercadobitcoin.net/ws,
//     "A User-Agent header SHOULD be sent", idle connections closed after 5 s)
//   - ping/subscribe: https://ws.mercadobitcoin.net/docs/v0/#/api/GeneralMessages
//   - trade:        https://ws.mercadobitcoin.net/docs/v0/#/api/PublicMessages
//   - errors:       https://ws.mercadobitcoin.net/docs/v0/#/api/Errors
//
// The REST API (https://api.mercadobitcoin.net/api/v4/docs, GET /{symbol}/trades)
// is not used: the WebSocket carries every field needed, and REST allows one
// request per second per endpoint (500/min in total), which cannot keep up with
// several markets. See docs/adr/0006-mercado-bitcoin-adapter.md.
package mercadobitcoin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// Exchange identifies Mercado Bitcoin in stream keys, logs and metrics. It
// doubles as the LEAN market name; LEAN has no Mercado Bitcoin market, so
// "mercadobitcoin" must be registered in LEAN as a custom market (Market.Add)
// before a backtest can read it; see docs/adr/0006.
const Exchange = "mercadobitcoin"

// DefaultURL is the production WebSocket endpoint.
const DefaultURL = "wss://ws.mercadobitcoin.net/ws"

const (
	// pingInterval: the server sends no heartbeat and a market can go a minute
	// without a trade, so the client sends {"type":"ping"} and the server's
	// {"type":"pong"} proves the connection is alive.
	pingInterval = 5 * time.Second
	// defaultReadTimeout tolerates two lost pongs.
	defaultReadTimeout = 15 * time.Second
	userAgent          = "tekel (+https://github.com/Amadeus-22/tekel)"
)

var appPing = []byte(`{"type":"ping"}`)

// symbolRE is Mercado Bitcoin's REST spelling BASE-QUOTE ("BTC-BRL"). It is
// the symbol Tekel reports and writes (LEAN ticker "btcbrl").
var symbolRE = regexp.MustCompile(`^[A-Z0-9]{1,12}-[A-Z0-9]{1,12}$`)

// MarketID converts "BTC-BRL" to the WebSocket market id "BRLBTC" (quote
// first), the spelling of the subscribe examples and of live trade messages.
func MarketID(symbol string) string {
	base, quote, _ := strings.Cut(symbol, "-")
	return quote + base
}

// Config configures the feed.
type Config struct {
	URL string
	// Symbols in Mercado Bitcoin's BASE-QUOTE spelling, e.g. "BTC-BRL".
	Symbols []string
	// Conn holds timeouts and backoff; Name, URL, header and pings are set by New.
	Conn   wsconn.Config
	Logger *slog.Logger
	// DecodeErrors is called once per malformed message; nil is allowed.
	DecodeErrors func()
}

// Feed implements app.Feed for Mercado Bitcoin.
type Feed struct {
	cfg       Config
	ids       map[string]string // WebSocket market id → symbol
	malformed *wsconn.Malformed
}

// New validates cfg and returns a feed.
func New(cfg Config) (*Feed, error) {
	if err := wsconn.CheckSymbols(Exchange, cfg.Symbols, symbolRE, "look like BTC-BRL"); err != nil {
		return nil, err
	}
	ids := make(map[string]string, len(cfg.Symbols))
	for _, s := range cfg.Symbols {
		ids[MarketID(s)] = s
	}
	if cfg.Logger == nil {
		return nil, errors.New("mercadobitcoin: logger is required")
	}
	cfg.Conn.CapReadTimeout(defaultReadTimeout)
	cfg.Conn.PingInterval = pingInterval
	cfg.Conn.AppPing = appPing
	cfg.Conn.Header = http.Header{"User-Agent": {userAgent}}
	cfg.Conn.Bind(Exchange, cfg.URL, cfg.Logger)
	return &Feed{cfg: cfg, ids: ids, malformed: wsconn.NewMalformed(Exchange, cfg.Logger, cfg.DecodeErrors)}, nil
}

// Exchange implements app.Feed.
func (f *Feed) Exchange() string { return Exchange }

// Symbols implements app.Feed.
func (f *Feed) Symbols() []string { return append([]string(nil), f.cfg.Symbols...) }

// Kind implements app.Feed: the feed carries trades.
func (f *Feed) Kind() domain.StreamKind { return domain.StreamTrades }

// SubscribeRequest is the documented request, one per market:
//
//	{"type":"subscribe","subscription":{"name":"trade","id":"BRLBTC"}}
type SubscribeRequest struct {
	Type         string       `json:"type"`
	Subscription Subscription `json:"subscription"`
}

// Subscription is the "subscription" of a SubscribeRequest.
type Subscription struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

// NewSubscribeRequest returns the trade subscription for symbol ("BTC-BRL").
func NewSubscribeRequest(symbol string) SubscribeRequest {
	return SubscribeRequest{Type: "subscribe", Subscription: Subscription{Name: "trade", ID: MarketID(symbol)}}
}

// Run implements app.Feed.
func (f *Feed) Run(ctx context.Context, emit app.Emit) error {
	return wsconn.Run(ctx, f.cfg.Conn, wsconn.Handlers{
		Subscribe: func(_ context.Context, conn *websocket.Conn) error {
			for _, s := range f.cfg.Symbols {
				if err := conn.WriteJSON(NewSubscribeRequest(s)); err != nil {
					return err
				}
			}
			return nil
		},
		Up: func(ctx context.Context, at time.Time) error {
			return emit(ctx, app.Event{Kind: app.EventConnected, At: at})
		},
		Down: func(ctx context.Context, at time.Time, cause error) error {
			return emit(ctx, app.Event{Kind: app.EventDisconnected, At: at, Err: cause})
		},
		Message: func(ctx context.Context, msg []byte, received time.Time) error {
			m, err := Decode(msg, received, f.ids)
			if err != nil {
				f.malformed.Report(msg, received, err)
				return nil
			}
			if m.Error != "" {
				// The server's errors are "unknown message type", "invalid JSON",
				// "already subscribed", "not subscribed" and, for an unknown market,
				// "invalid pair to subscribe": all mean our requests are wrong, and
				// reconnecting would repeat them.
				return wsconn.Permanent(fmt.Errorf("server error: %s (symbols %s)", m.Error, strings.Join(f.cfg.Symbols, ",")))
			}
			if m.Trade == nil {
				return nil
			}
			return emit(ctx, app.Event{Kind: app.EventTrade, At: received, Trade: *m.Trade})
		},
	})
}

// Message is the part of one decoded message Tekel acts on.
type Message struct {
	// Trade is set for a "trade" event.
	Trade *domain.Trade
	// Error is the server's message for a {"type":"error"} reply.
	Error string
}

// envelope covers every documented shape:
//
//	{"type":"trade","id":"BRLBTC","ts":1607685693917511270,"data":{...}}
//	{"type":"pong"}
//	{"type":"error","message":"invalid pair to subscribe"}
//	{"id":"BRLBTC","name":"trade"}              (subscribe acknowledgement)
type envelope struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
}

// tradeData is "data" of a trade event:
//
//	{"tid":8522200,"date":1613000055,"type":"sell","price":244500.03022,"amount":0.00052}
//
// tid is the trade id, consecutive per market (checked live against the REST
// history: 1000 trades of BTC-BRL and USDT-BRL without a hole), so sequence_gap
// applies. date is whole Unix seconds; "ts" of the envelope is only the send
// time. type is the taker side. price and amount are JSON numbers; decoding them
// as json.Number keeps the literal text, so float64 never touches them.
type tradeData struct {
	TID    *int64      `json:"tid"`
	Date   *int64      `json:"date"`
	Type   string      `json:"type"`
	Price  json.Number `json:"price"`
	Amount json.Number `json:"amount"`
}

// Decode parses one message. ids maps subscribed WebSocket market ids to
// symbols; a trade for another market is an error. Pongs, acknowledgements and
// other event types decode to an empty Message.
func Decode(msg []byte, received time.Time, ids map[string]string) (Message, error) {
	var env envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		return Message{}, fmt.Errorf("decode envelope: %w", err)
	}
	switch env.Type {
	case "error":
		if env.Message == "" {
			env.Message = "unspecified error"
		}
		return Message{Error: env.Message}, nil
	case "trade":
	default:
		return Message{}, nil
	}
	symbol, ok := ids[env.ID]
	if !ok {
		return Message{}, fmt.Errorf("trade for unsubscribed market %q", env.ID)
	}
	var d tradeData
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return Message{}, fmt.Errorf("decode trade data: %w", err)
	}
	t, err := d.trade(symbol, received)
	if err != nil {
		return Message{}, fmt.Errorf("decode trade: %w", err)
	}
	return Message{Trade: &t}, nil
}

func (d tradeData) trade(symbol string, received time.Time) (domain.Trade, error) {
	if d.TID == nil {
		return domain.Trade{}, errors.New("missing tid")
	}
	if d.Date == nil || *d.Date <= 0 {
		return domain.Trade{}, errors.New("missing date")
	}
	price, err := domain.ParseDecimal(d.Price.String())
	if err != nil {
		return domain.Trade{}, fmt.Errorf("price: %w", err)
	}
	qty, err := domain.ParseDecimal(d.Amount.String())
	if err != nil {
		return domain.Trade{}, fmt.Errorf("amount: %w", err)
	}
	side, err := domain.ParseSide(d.Type)
	if err != nil {
		return domain.Trade{}, err
	}
	t := domain.Trade{
		Exchange: Exchange, Symbol: symbol, TradeID: *d.TID,
		Price: price, Qty: qty, Side: side,
		ExchangeTime: time.Unix(*d.Date, 0).UTC(), ReceivedTime: received.UTC(),
	}
	if err := t.Validate(); err != nil {
		return domain.Trade{}, err
	}
	return t, nil
}

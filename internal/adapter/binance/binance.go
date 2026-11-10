// Package binance is the Binance spot trade feed: one combined-stream WebSocket
// carrying <symbol>@trade for every configured symbol.
//
// Message formats follow Binance's official documentation, "WebSocket Streams
// for Binance" → "Trade Streams" and "General WSS information":
// https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md
// (also https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams).
package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// Exchange identifies Binance in stream keys, logs and metrics. It doubles as
// the LEAN market name, so it must match LEAN's Common/Market.cs constant.
const Exchange = "binance"

// DefaultBaseURL is the public market-data endpoint.
const DefaultBaseURL = "wss://stream.binance.com:9443"

var symbolRE = regexp.MustCompile(`^[A-Z0-9]{2,20}$`)

// Config configures the feed.
type Config struct {
	// BaseURL is the endpoint without path, e.g. DefaultBaseURL.
	BaseURL string
	// Symbols in Binance's spelling, e.g. "BTCUSDT".
	Symbols []string
	// Conn holds timeouts and backoff; Name and URL are filled in by New.
	Conn   wsconn.Config
	Logger *slog.Logger
	// DecodeErrors is called once per malformed message; nil is allowed.
	DecodeErrors func()
}

// Feed implements app.Feed for Binance.
type Feed struct {
	cfg       Config
	malformed *wsconn.Malformed
}

// New validates cfg and returns a feed.
func New(cfg Config) (*Feed, error) {
	if err := wsconn.CheckSymbols(Exchange, cfg.Symbols, symbolRE, "be uppercase letters and digits, e.g. BTCUSDT"); err != nil {
		return nil, err
	}
	if cfg.Logger == nil {
		return nil, errors.New("binance: logger is required")
	}
	cfg.Conn.Bind(Exchange, StreamURL(cfg.BaseURL, cfg.Symbols), cfg.Logger)
	return &Feed{cfg: cfg, malformed: wsconn.NewMalformed(Exchange, cfg.Logger, cfg.DecodeErrors)}, nil
}

// StreamURL builds the combined-stream URL. Subscribing through the URL means a
// reconnect resubscribes by construction.
// Docs: "Combined streams are accessed at /stream?streams=<streamName1>/<streamName2>"
// and "All symbols for streams are lowercase".
func StreamURL(base string, symbols []string) string {
	names := make([]string, len(symbols))
	for i, s := range symbols {
		names[i] = strings.ToLower(s) + "@trade"
	}
	return strings.TrimRight(base, "/") + "/stream?streams=" + strings.Join(names, "/")
}

// Exchange implements app.Feed.
func (f *Feed) Exchange() string { return Exchange }

// Symbols implements app.Feed.
func (f *Feed) Symbols() []string { return append([]string(nil), f.cfg.Symbols...) }

// Kind implements app.Feed: the feed carries trades.
func (f *Feed) Kind() domain.StreamKind { return domain.StreamTrades }

// Run implements app.Feed.
func (f *Feed) Run(ctx context.Context, emit app.Emit) error {
	return wsconn.Run(ctx, f.cfg.Conn, wsconn.Handlers{
		Up: func(ctx context.Context, at time.Time) error {
			return emit(ctx, app.Event{Kind: app.EventConnected, At: at})
		},
		Down: func(ctx context.Context, at time.Time, cause error) error {
			return emit(ctx, app.Event{Kind: app.EventDisconnected, At: at, Err: cause})
		},
		Message: func(ctx context.Context, msg []byte, received time.Time) error {
			t, ok, err := Decode(msg, received)
			if err != nil {
				f.malformed.Report(msg, received, err)
				return nil
			}
			if !ok {
				return nil
			}
			return emit(ctx, app.Event{Kind: app.EventTrade, At: received, Trade: t})
		},
	})
}

// envelope is the combined-stream wrapper: {"stream":"<streamName>","data":<rawPayload>}.
type envelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

// tradeEvent is the <symbol>@trade payload:
//
//	{"e":"trade","E":1672515782136,"s":"BNBBTC","t":12345,"p":"0.001","q":"100",
//	 "T":1672515782136,"m":true,"M":true}
//
// E is the event time and T the trade time, both in milliseconds by default;
// m is "Is the buyer the market maker?"; M is documented as "Ignore".
//
// Every key is declared, including E and M which Tekel does not use:
// encoding/json matches keys case-insensitively when no exact field exists, so
// without them "E" (a number) would land in the "e" string field and "M" in "m".
type tradeEvent struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   *int64 `json:"t"`
	Price     string `json:"p"`
	Qty       string `json:"q"`
	TradeTime *int64 `json:"T"`
	BuyerIsMM *bool  `json:"m"`
	Ignore    bool   `json:"M"`
}

// Decode parses one combined-stream message. ok is false for well-formed events
// that are not trades (e.g. "serverShutdown"), which the caller skips.
func Decode(msg []byte, received time.Time) (t domain.Trade, ok bool, err error) {
	var env envelope
	if err := json.Unmarshal(msg, &env); err != nil {
		return domain.Trade{}, false, fmt.Errorf("decode envelope: %w", err)
	}
	if len(env.Data) == 0 {
		return domain.Trade{}, false, errors.New("decode envelope: missing data")
	}
	var ev tradeEvent
	if err := json.Unmarshal(env.Data, &ev); err != nil {
		return domain.Trade{}, false, fmt.Errorf("decode %s: %w", env.Stream, err)
	}
	if ev.Event != "trade" {
		return domain.Trade{}, false, nil
	}
	if ev.TradeID == nil || ev.TradeTime == nil || ev.BuyerIsMM == nil {
		return domain.Trade{}, false, fmt.Errorf("decode %s: missing t, T or m", env.Stream)
	}
	price, err := domain.ParseDecimal(ev.Price)
	if err != nil {
		return domain.Trade{}, false, fmt.Errorf("decode %s price: %w", env.Stream, err)
	}
	qty, err := domain.ParseDecimal(ev.Qty)
	if err != nil {
		return domain.Trade{}, false, fmt.Errorf("decode %s quantity: %w", env.Stream, err)
	}
	// If the buyer was the maker, the seller took liquidity: the taker sold.
	side := domain.SideBuy
	if *ev.BuyerIsMM {
		side = domain.SideSell
	}
	t = domain.Trade{
		Exchange: Exchange, Symbol: ev.Symbol, TradeID: *ev.TradeID,
		Price: price, Qty: qty, Side: side,
		ExchangeTime: time.UnixMilli(*ev.TradeTime).UTC(), ReceivedTime: received.UTC(),
	}
	if err := t.Validate(); err != nil {
		return domain.Trade{}, false, fmt.Errorf("decode %s: %w", env.Stream, err)
	}
	return t, true, nil
}

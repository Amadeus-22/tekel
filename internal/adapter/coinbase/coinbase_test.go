package coinbase

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/feedtest"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn/wstest"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// recorded returns the messages of testdata/feed.jsonl, recorded from the live
// feed on 2026-11-24: an error for an unknown product, the subscriptions
// reply, one last_match, four matches (BTC-USD and ETH-USD) and a heartbeat.
func recorded(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/feed.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		lines = append(lines, s.Text())
	}
	return lines
}

func TestDecodeRecordedFeed(t *testing.T) {
	recv := time.Date(2026, 11, 24, 19, 52, 12, 0, time.UTC)
	var trades []domain.Trade
	var subErrs []string
	for _, line := range recorded(t) {
		m, err := Decode([]byte(line), recv)
		if err != nil {
			t.Fatalf("Decode(%s) = %v", line, err)
		}
		if m.Trade != nil {
			trades = append(trades, *m.Trade)
		}
		if m.SubscribeError != "" {
			subErrs = append(subErrs, m.SubscribeError)
		}
	}
	if len(trades) != 4 {
		t.Fatalf("trades = %d, want the 4 matches (last_match is skipped)", len(trades))
	}
	if !slices.Equal(subErrs, []string{"NOPE-XYZ is not a valid product"}) {
		t.Errorf("subscribe errors = %v", subErrs)
	}
	first := trades[0]
	if first.Exchange != "coinbase" || first.Symbol != "BTC-USD" || first.TradeID != 1145969290 ||
		first.Price.String() != "59681.87" || first.Qty.String() != "0.00000004" {
		t.Errorf("first trade = %+v", first)
	}
	if first.Side != domain.SideSell {
		t.Errorf("side = %s, want sell: the maker bought, so the taker sold", first.Side)
	}
	if want := time.Date(2026, 11, 24, 19, 52, 11, 373227000, time.UTC); !first.ExchangeTime.Equal(want) || !first.ReceivedTime.Equal(recv) {
		t.Errorf("times = %v / %v", first.ExchangeTime, first.ReceivedTime)
	}
}

func TestDecodeRejectsBadMatches(t *testing.T) {
	recv := time.Now()
	for name, msg := range map[string]string{
		"not json":      `{`,
		"no trade_id":   `{"type":"match","product_id":"BTC-USD","price":"1","size":"1","side":"buy","time":"2026-11-24T19:52:11Z"}`,
		"bad price":     `{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"x","size":"1","side":"buy","time":"2026-11-24T19:52:11Z"}`,
		"zero price":    `{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"0","size":"1","side":"buy","time":"2026-11-24T19:52:11Z"}`,
		"bad size":      `{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"1","size":"","side":"buy","time":"2026-11-24T19:52:11Z"}`,
		"unknown side":  `{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"1","size":"1","side":"hold","time":"2026-11-24T19:52:11Z"}`,
		"bad timestamp": `{"type":"match","trade_id":1,"product_id":"BTC-USD","price":"1","size":"1","side":"buy","time":"yesterday"}`,
	} {
		if _, err := Decode([]byte(msg), recv); err == nil {
			t.Errorf("%s: Decode() = nil error", name)
		}
	}
}

func TestSubscribeRequestShape(t *testing.T) {
	got := NewSubscribeRequest([]string{"BTC-USD"})
	if got.Type != "subscribe" || !slices.Equal(got.ProductIDs, []string{"BTC-USD"}) ||
		!slices.Equal(got.Channels, []string{"matches", "heartbeat"}) {
		t.Errorf("request = %+v", got)
	}
}

func TestNewValidates(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for name, tt := range map[string]struct {
		cfg  Config
		want string
	}{
		"no symbols": {Config{Logger: log}, "coinbase: at least one symbol is required"},
		"slash":      {Config{Symbols: []string{"BTC/USD"}, Logger: log}, `coinbase: symbol "BTC/USD" must look like BTC-USD`},
		"lowercase":  {Config{Symbols: []string{"btc-usd"}, Logger: log}, `coinbase: symbol "btc-usd" must look like BTC-USD`},
		"no logger":  {Config{Symbols: []string{"BTC-USD"}}, "coinbase: logger is required"},
	} {
		if _, err := New(tt.cfg); err == nil || err.Error() != tt.want {
			t.Errorf("%s: New() = %v, want %q", name, err, tt.want)
		}
	}
	f, err := New(Config{URL: DefaultURL, Symbols: []string{"BTC-USD"}, Logger: log, Conn: wsconn.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if f.Exchange() != "coinbase" || f.Symbols()[0] != "BTC-USD" || f.Kind() != domain.StreamTrades ||
		f.cfg.Conn.ReadTimeout != defaultReadTimeout {
		t.Errorf("feed = %s %v %s read timeout %v", f.Exchange(), f.Symbols(), f.Kind(), f.cfg.Conn.ReadTimeout)
	}
}

func TestFeedSubscribesOnEveryConnection(t *testing.T) {
	lines := recorded(t)[1:] // without the error for the unknown product
	subs := make(chan string, 4)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		subs <- string(msg)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"match","trade_id":1}`)) // malformed
		for _, l := range lines {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(l))
		}
		if n == 1 {
			return
		}
		wstest.DrainUntilClosed(conn)
	})

	var decodeErrors atomic.Int32
	f, err := New(Config{URL: srv.URL(""), Symbols: []string{"BTC-USD", "ETH-USD"}, Conn: feedtest.FastConn(),
		Logger: slog.New(slog.DiscardHandler), DecodeErrors: func() { decodeErrors.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}
	events, err := feedtest.Collect(t, f, 11)
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	for i := 0; i < 2; i++ {
		got := <-subs
		if !strings.Contains(got, `"matches"`) || !strings.Contains(got, `"BTC-USD"`) || !strings.Contains(got, `"ETH-USD"`) {
			t.Errorf("subscribe %d = %s", i+1, got)
		}
	}
	trade := app.EventTrade
	want := []app.EventKind{app.EventConnected, trade, trade, trade, trade, app.EventDisconnected,
		app.EventConnected, trade, trade, trade, trade}
	if got := feedtest.Kinds(events); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if decodeErrors.Load() < 1 {
		t.Error("malformed match was not counted")
	}
}

func TestFeedStopsOnRejectedSubscription(t *testing.T) {
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(recorded(t)[0]))
		wstest.DrainUntilClosed(conn)
	})
	f, _ := New(Config{URL: srv.URL(""), Symbols: []string{"NOPE-XYZ"}, Conn: feedtest.FastConn(), Logger: slog.New(slog.DiscardHandler)})
	err := f.Run(context.Background(), func(context.Context, app.Event) error { return nil })
	var perr *wsconn.PermanentError
	if !errors.As(err, &perr) || !strings.Contains(err.Error(), "NOPE-XYZ is not a valid product") {
		t.Fatalf("Run() = %v, want a permanent subscribe error", err)
	}
}

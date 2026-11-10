package kraken

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

// Examples from https://docs.kraken.com/api/docs/websocket-v2/trade
const (
	docUpdate = `{"channel":"trade","type":"update","data":[{"symbol":"MATIC/USD","side":"sell","price":0.5117,"qty":40.0,"ord_type":"market","trade_id":4665906,"timestamp":"2023-09-25T07:49:37.708706Z"}]}`
	docAck    = `{"method":"subscribe","result":{"channel":"trade","snapshot":true,"symbol":"MATIC/USD"},"success":true,"time_in":"2023-09-25T09:21:10.428340Z","time_out":"2023-09-25T09:21:10.428375Z"}`
)

func TestDecode(t *testing.T) {
	recv := time.Date(2023, 9, 25, 7, 49, 38, 0, time.UTC)
	tests := []struct {
		name       string
		msg        string
		wantTrades int
		wantSubErr string
		wantErr    bool
	}{
		{name: "documented update", msg: docUpdate, wantTrades: 1},
		{name: "snapshot with two trades", wantTrades: 2, msg: `{"channel":"trade","type":"snapshot","data":[` +
			`{"symbol":"BTC/USD","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2023-09-25T07:48:36.925533Z"},` +
			`{"symbol":"BTC/USD","side":"buy","price":2,"qty":1,"trade_id":2,"timestamp":"2023-09-25T07:48:36.925533Z"}]}`},
		{name: "subscribe ack", msg: docAck},
		{name: "subscribe rejected", msg: `{"method":"subscribe","success":false,"error":"Currency pair not supported","symbol":"ABC/USD"}`,
			wantSubErr: "Currency pair not supported (symbol ABC/USD)"},
		{name: "heartbeat", msg: `{"channel":"heartbeat"}`},
		{name: "status", msg: `{"channel":"status","type":"update","data":[{"system":"online"}]}`},
		{name: "not json", msg: `nope`, wantErr: true},
		{name: "unknown trade type", msg: `{"channel":"trade","type":"delta","data":[]}`, wantErr: true},
		{name: "data not a list", msg: `{"channel":"trade","type":"update","data":{}}`, wantErr: true},
		{name: "missing trade id", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"symbol":"A/B","side":"buy","price":1,"qty":1,"timestamp":"2023-09-25T07:48:36Z"}]}`},
		{name: "bad side", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"symbol":"A/B","side":"up","price":1,"qty":1,"trade_id":1,"timestamp":"2023-09-25T07:48:36Z"}]}`},
		{name: "negative price", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"symbol":"A/B","side":"buy","price":-1,"qty":1,"trade_id":1,"timestamp":"2023-09-25T07:48:36Z"}]}`},
		{name: "missing qty", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"symbol":"A/B","side":"buy","price":1,"trade_id":1,"timestamp":"2023-09-25T07:48:36Z"}]}`},
		{name: "bad timestamp", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"symbol":"A/B","side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"yesterday"}]}`},
		{name: "missing symbol", wantErr: true, msg: `{"channel":"trade","type":"update","data":[{"side":"buy","price":1,"qty":1,"trade_id":1,"timestamp":"2023-09-25T07:48:36Z"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Decode([]byte(tt.msg), recv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(m.Trades) != tt.wantTrades || m.SubscribeError != tt.wantSubErr {
				t.Fatalf("Decode() = %d trades, sub error %q", len(m.Trades), m.SubscribeError)
			}
		})
	}
}

func TestDecodeKeepsExactDecimalsAndFields(t *testing.T) {
	msg := `{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":64250.10000,"qty":0.00012345,"ord_type":"limit","trade_id":90210,"timestamp":"2024-01-02T03:04:05.123456Z"}]}`
	recv := time.Date(2024, 1, 2, 3, 4, 5, 200_000_000, time.UTC)
	m, err := Decode([]byte(msg), recv)
	if err != nil {
		t.Fatal(err)
	}
	tr := m.Trades[0]
	if tr.Price.String() != "64250.10000" || tr.Qty.String() != "0.00012345" {
		t.Errorf("decimals = %s / %s, want the literal text", tr.Price, tr.Qty)
	}
	want := time.Date(2024, 1, 2, 3, 4, 5, 123_456_000, time.UTC)
	if tr.Exchange != "kraken" || tr.Symbol != "BTC/USD" || tr.TradeID != 90210 || tr.Side != domain.SideBuy ||
		!tr.ExchangeTime.Equal(want) || !tr.ReceivedTime.Equal(recv) {
		t.Errorf("trade = %+v", tr)
	}
}

func TestSubscribeRequestShape(t *testing.T) {
	b, err := json.Marshal(NewSubscribeRequest([]string{"BTC/USD", "ETH/USD"}))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"method":"subscribe","params":{"channel":"trade","symbol":["BTC/USD","ETH/USD"],"snapshot":false}}`
	if string(b) != want {
		t.Errorf("subscribe = %s\nwant        %s", b, want)
	}
}

func TestNewValidates(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for name, tt := range map[string]struct {
		cfg  Config
		want string
	}{
		"no symbols": {Config{Logger: log}, "kraken: at least one symbol is required"},
		"no slash":   {Config{Symbols: []string{"BTCUSD"}, Logger: log}, `kraken: symbol "BTCUSD" must look like BTC/USD`},
		"lowercase":  {Config{Symbols: []string{"btc/usd"}, Logger: log}, `kraken: symbol "btc/usd" must look like BTC/USD`},
		"no logger":  {Config{Symbols: []string{"BTC/USD"}}, "kraken: logger is required"},
	} {
		if _, err := New(tt.cfg); err == nil || err.Error() != tt.want {
			t.Errorf("%s: New() = %v, want %q", name, err, tt.want)
		}
	}
	f, err := New(Config{URL: DefaultURL, Symbols: []string{"BTC/USD"}, Logger: log, Conn: wsconn.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	if f.Exchange() != "kraken" || f.Symbols()[0] != "BTC/USD" || f.cfg.Conn.ReadTimeout != defaultReadTimeout {
		t.Errorf("feed = %s %v read timeout %v", f.Exchange(), f.Symbols(), f.cfg.Conn.ReadTimeout)
	}
}

func TestFeedSubscribesOnEveryConnection(t *testing.T) {
	subs := make(chan string, 4)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		subs <- string(msg)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(docAck))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"channel":"heartbeat"}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"channel":"trade","type":"update","data":[1]}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(docUpdate))
		if n == 1 {
			return
		}
		wstest.DrainUntilClosed(conn)
	})

	var decodeErrors atomic.Int32
	f, err := New(Config{URL: srv.URL("/v2"), Symbols: []string{"MATIC/USD"}, Conn: feedtest.FastConn(),
		Logger: slog.New(slog.DiscardHandler), DecodeErrors: func() { decodeErrors.Add(1) }})
	if err != nil {
		t.Fatal(err)
	}

	events, err := feedtest.Collect(t, f, 5)
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	for _, ev := range events {
		if ev.Kind == app.EventTrade && ev.Trade.TradeID != 4665906 {
			t.Errorf("trade = %+v", ev.Trade)
		}
	}

	for i := 0; i < 2; i++ {
		got := <-subs
		if !strings.Contains(got, `"channel":"trade"`) || !strings.Contains(got, `"MATIC/USD"`) {
			t.Errorf("subscribe %d = %s", i+1, got)
		}
	}
	want := []app.EventKind{app.EventConnected, app.EventTrade, app.EventDisconnected, app.EventConnected, app.EventTrade}
	if got := feedtest.Kinds(events); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if decodeErrors.Load() < 1 {
		t.Error("malformed trade message was not counted")
	}
}

func TestFeedStopsOnRejectedSubscription(t *testing.T) {
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage,
			[]byte(`{"method":"subscribe","success":false,"error":"Currency pair not supported","symbol":"ABC/USD"}`))
		wstest.DrainUntilClosed(conn)
	})
	f, _ := New(Config{URL: srv.URL("/v2"), Symbols: []string{"ABC/USD"}, Conn: feedtest.FastConn(), Logger: slog.New(slog.DiscardHandler)})
	err := f.Run(context.Background(), func(context.Context, app.Event) error { return nil })
	var perr *wsconn.PermanentError
	if !errors.As(err, &perr) || !strings.Contains(err.Error(), "Currency pair not supported") {
		t.Fatalf("Run() = %v, want a permanent subscribe error", err)
	}
}

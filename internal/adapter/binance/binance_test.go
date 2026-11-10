package binance

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn/wstest"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

// docExample is the payload from Binance's "Trade Streams" documentation,
// wrapped in the combined-stream envelope.
const docExample = `{"stream":"bnbbtc@trade","data":{"e":"trade","E":1672515782136,"s":"BNBBTC","t":12345,"p":"0.001","q":"100","T":1672515782136,"m":true,"M":true}}`

func TestDecode(t *testing.T) {
	recv := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		msg     string
		wantOK  bool
		wantErr bool
		check   func(*testing.T, domain.Trade)
	}{
		{
			name: "documented example", msg: docExample, wantOK: true,
			check: func(t *testing.T, tr domain.Trade) {
				if tr.Exchange != "binance" || tr.Symbol != "BNBBTC" || tr.TradeID != 12345 {
					t.Errorf("identity = %+v", tr)
				}
				if tr.Price.String() != "0.001" || tr.Qty.String() != "100" {
					t.Errorf("price/qty = %s/%s", tr.Price, tr.Qty)
				}
				if tr.Side != domain.SideSell {
					t.Errorf("side = %s, want sell (buyer was maker)", tr.Side)
				}
				if !tr.ExchangeTime.Equal(time.UnixMilli(1672515782136)) || !tr.ReceivedTime.Equal(recv) {
					t.Errorf("times = %v / %v", tr.ExchangeTime, tr.ReceivedTime)
				}
			},
		},
		{
			name:   "taker buy",
			msg:    `{"stream":"btcusdt@trade","data":{"e":"trade","s":"BTCUSDT","t":1,"p":"17212.12000000","q":"0.00100000","T":1672515782136,"m":false}}`,
			wantOK: true,
			check: func(t *testing.T, tr domain.Trade) {
				if tr.Side != domain.SideBuy || tr.Price.String() != "17212.12000000" {
					t.Errorf("trade = %+v", tr)
				}
			},
		},
		{name: "server shutdown is skipped", msg: `{"stream":"x","data":{"e":"serverShutdown","E":1770123456789}}`},
		{name: "not json", msg: `{`, wantErr: true},
		{name: "no data", msg: `{"result":null,"id":1}`, wantErr: true},
		{name: "data not an object", msg: `{"stream":"x","data":[1]}`, wantErr: true},
		{name: "missing trade id", msg: `{"stream":"x","data":{"e":"trade","s":"A","p":"1","q":"1","T":1,"m":true}}`, wantErr: true},
		{name: "bad price", msg: `{"stream":"x","data":{"e":"trade","s":"A","t":1,"p":"-1","q":"1","T":1,"m":true}}`, wantErr: true},
		{name: "bad qty", msg: `{"stream":"x","data":{"e":"trade","s":"A","t":1,"p":"1","q":"","T":1,"m":true}}`, wantErr: true},
		{name: "zero price fails validation", msg: `{"stream":"x","data":{"e":"trade","s":"A","t":1,"p":"0","q":"1","T":1,"m":true}}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, ok, err := Decode([]byte(tt.msg), recv)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if ok != tt.wantOK {
				t.Fatalf("Decode() ok = %v, want %v", ok, tt.wantOK)
			}
			if tt.check != nil {
				tt.check(t, tr)
			}
		})
	}
}

func TestStreamURL(t *testing.T) {
	got := StreamURL("wss://stream.binance.com:9443/", []string{"BTCUSDT", "ETHUSDT"})
	want := "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
	if got != want {
		t.Errorf("StreamURL() = %q, want %q", got, want)
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestNewValidates(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no symbols", Config{Logger: discard()}, "binance: at least one symbol is required"},
		{"lowercase symbol", Config{Symbols: []string{"btcusdt"}, Logger: discard()},
			`binance: symbol "btcusdt" must be uppercase letters and digits, e.g. BTCUSDT`},
		{"slash symbol", Config{Symbols: []string{"BTC/USDT"}, Logger: discard()},
			`binance: symbol "BTC/USDT" must be uppercase letters and digits, e.g. BTCUSDT`},
		{"no logger", Config{Symbols: []string{"BTCUSDT"}}, "binance: logger is required"},
	}
	for _, tt := range tests {
		if _, err := New(tt.cfg); err == nil || err.Error() != tt.want {
			t.Errorf("%s: New() = %v, want %q", tt.name, err, tt.want)
		}
	}
	f, err := New(Config{Symbols: []string{"BTCUSDT"}, Logger: discard(), BaseURL: DefaultBaseURL})
	if err != nil {
		t.Fatal(err)
	}
	if f.Exchange() != "binance" || len(f.Symbols()) != 1 {
		t.Errorf("feed = %s %v", f.Exchange(), f.Symbols())
	}
}

func TestFeedRunAgainstLocalServer(t *testing.T) {
	gotPath := make(chan string, 2)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		gotPath <- "" // placeholder so the test knows a connection happened
		_ = conn.WriteMessage(websocket.TextMessage, []byte(docExample))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`garbage`))
		if n == 1 {
			return // drop: the feed must report the disconnect and reconnect
		}
		wstest.DrainUntilClosed(conn)
	})

	var decodeErrors atomic.Int32
	conn := wsconn.DefaultConfig()
	conn.BackoffMin, conn.BackoffMax = time.Millisecond, 5*time.Millisecond
	f, err := New(Config{
		BaseURL: srv.URL(""), Symbols: []string{"BNBBTC"}, Conn: conn, Logger: discard(),
		DecodeErrors: func() { decodeErrors.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	var kinds []app.EventKind
	enough := make(chan struct{})
	emit := func(_ context.Context, ev app.Event) error {
		mu.Lock()
		defer mu.Unlock()
		kinds = append(kinds, ev.Kind)
		if len(kinds) == 5 { // up, trade, down, up, trade
			close(enough)
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, emit) }()
	select {
	case <-enough:
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive the expected events")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []app.EventKind{app.EventConnected, app.EventTrade, app.EventDisconnected, app.EventConnected, app.EventTrade}
	for i, k := range want {
		if kinds[i] != k {
			t.Fatalf("events = %v, want prefix %v", kinds, want)
		}
	}
	if decodeErrors.Load() < 1 {
		t.Error("malformed message was not counted")
	}
}

func TestFeedRunStopsWhenEmitFails(t *testing.T) {
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) { wstest.DrainUntilClosed(conn) })
	f, _ := New(Config{BaseURL: srv.URL(""), Symbols: []string{"BTCUSDT"}, Conn: wsconn.DefaultConfig(), Logger: discard()})
	ctx, cancel := context.WithCancel(context.Background())
	emit := func(context.Context, app.Event) error {
		cancel() // the pipeline is shutting down
		return errors.New("context canceled")
	}
	if err := f.Run(ctx, emit); err != nil {
		t.Errorf("Run() = %v, want nil on shutdown", err)
	}
}

package mercadobitcoin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn/wstest"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

// Examples from https://ws.mercadobitcoin.net/docs/v0/ (PublicMessages, GeneralMessages, Errors).
const (
	docTrade = `{"type":"trade","id":"BRLBTC","ts":1607685693917511270,"data":{"tid":8522200,"date":1613000055,"type":"sell","price":244500.03022,"amount":0.00052}}`
	docAck   = `{"id":"BRLBTC","name":"trade"}`
	docPong  = `{"type":"pong"}`
	docError = `{"type":"error","message":"already subscribed"}`
)

// Recorded from wss://ws.mercadobitcoin.net/ws on 2026-09-29 (tickguard probe).
const (
	liveTradeBTC   = `{"type":"trade","id":"BRLBTC","ts":1790720762225295243,"data":{"tid":22586953,"date":1790720762,"type":"buy","price":435832,"amount":0.00000222}}`
	liveTradeBTC2  = `{"type":"trade","id":"BRLBTC","ts":1790720762262275368,"data":{"tid":22586954,"date":1790720762,"type":"sell","price":435837,"amount":0.00003}}`
	liveTradeUSDT  = `{"type":"trade","id":"BRLUSDT","ts":1790720779821012436,"data":{"tid":1556193,"date":1790720779,"type":"sell","price":5.2098,"amount":27.4319}}`
	liveBadPairErr = `{"type":"error","message":"invalid pair to subscribe"}`
)

var ids = map[string]string{"BRLBTC": "BTC-BRL", "BRLUSDT": "USDT-BRL"}

func TestDecode(t *testing.T) {
	recv := time.Date(2026, 9, 29, 22, 26, 2, 300_000_000, time.UTC)
	tests := []struct {
		name      string
		msg       string
		wantTrade bool
		wantErrMs string
		wantErr   bool
	}{
		{name: "documented trade", msg: docTrade, wantTrade: true},
		{name: "live trade", msg: liveTradeBTC, wantTrade: true},
		{name: "live usdt trade", msg: liveTradeUSDT, wantTrade: true},
		{name: "subscribe ack", msg: docAck},
		{name: "pong", msg: docPong},
		{name: "ticker ignored", msg: `{"type":"ticker","id":"BRLBTC","ts":1,"data":{"last":"1"}}`},
		{name: "documented error", msg: docError, wantErrMs: "already subscribed"},
		{name: "live bad pair", msg: liveBadPairErr, wantErrMs: "invalid pair to subscribe"},
		{name: "error without message", msg: `{"type":"error"}`, wantErrMs: "unspecified error"},
		{name: "not json", msg: `nope`, wantErr: true},
		{name: "unsubscribed market", msg: strings.Replace(docTrade, "BRLBTC", "BRLETH", 1), wantErr: true},
		{name: "data not an object", msg: `{"type":"trade","id":"BRLBTC","data":[1]}`, wantErr: true},
		{name: "missing tid", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"date":1,"type":"buy","price":1,"amount":1}}`},
		{name: "missing date", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"tid":1,"type":"buy","price":1,"amount":1}}`},
		{name: "bad side", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"tid":1,"date":1,"type":"up","price":1,"amount":1}}`},
		{name: "negative price", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"tid":1,"date":1,"type":"buy","price":-1,"amount":1}}`},
		{name: "zero amount", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"tid":1,"date":1,"type":"buy","price":1,"amount":0}}`},
		{name: "missing amount", wantErr: true, msg: `{"type":"trade","id":"BRLBTC","data":{"tid":1,"date":1,"type":"buy","price":1}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Decode([]byte(tt.msg), recv, ids)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if (m.Trade != nil) != tt.wantTrade || m.Error != tt.wantErrMs {
				t.Fatalf("Decode() = %+v", m)
			}
		})
	}
}

func TestDecodeKeepsExactDecimalsAndFields(t *testing.T) {
	recv := time.Date(2026, 9, 29, 22, 26, 2, 229_000_000, time.UTC)
	m, err := Decode([]byte(liveTradeBTC), recv, ids)
	if err != nil {
		t.Fatal(err)
	}
	tr := *m.Trade
	if tr.Price.String() != "435832" || tr.Qty.String() != "0.00000222" {
		t.Errorf("decimals = %s / %s, want the literal text", tr.Price, tr.Qty)
	}
	if tr.Exchange != "mercadobitcoin" || tr.Symbol != "BTC-BRL" || tr.TradeID != 22586953 || tr.Side != domain.SideBuy ||
		!tr.ExchangeTime.Equal(time.Unix(1790720762, 0)) || !tr.ReceivedTime.Equal(recv) {
		t.Errorf("trade = %+v", tr)
	}
	m, _ = Decode([]byte(docTrade), recv, ids)
	if m.Trade.Price.String() != "244500.03022" || m.Trade.Side != domain.SideSell {
		t.Errorf("documented trade = %+v", m.Trade)
	}
}

func TestSubscribeRequestShape(t *testing.T) {
	b, err := json.Marshal(NewSubscribeRequest("BTC-BRL"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"subscribe","subscription":{"name":"trade","id":"BRLBTC"}}`
	if string(b) != want {
		t.Errorf("subscribe = %s\nwant        %s", b, want)
	}
	if MarketID("USDT-BRL") != "BRLUSDT" {
		t.Errorf("MarketID = %s", MarketID("USDT-BRL"))
	}
}

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestNewValidates(t *testing.T) {
	for name, cfg := range map[string]Config{
		"no symbols":    {Logger: discard()},
		"no dash":       {Symbols: []string{"BTCBRL"}, Logger: discard()},
		"lowercase":     {Symbols: []string{"btc-brl"}, Logger: discard()},
		"ws id spelled": {Symbols: []string{"BRLBTC"}, Logger: discard()},
		"no logger":     {Symbols: []string{"BTC-BRL"}},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New() = nil error", name)
		}
	}
	f, err := New(Config{URL: DefaultURL, Symbols: []string{"BTC-BRL"}, Logger: discard(), Conn: wsconn.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	c := f.cfg.Conn
	if f.Exchange() != "mercadobitcoin" || f.Symbols()[0] != "BTC-BRL" || c.ReadTimeout != defaultReadTimeout ||
		c.PingInterval != pingInterval || string(c.AppPing) != `{"type":"ping"}` || c.Header.Get("User-Agent") == "" {
		t.Errorf("feed = %s %v conn %+v", f.Exchange(), f.Symbols(), c)
	}
}

func testConn() wsconn.Config {
	c := wsconn.DefaultConfig()
	c.BackoffMin, c.BackoffMax = time.Millisecond, 5*time.Millisecond
	return c
}

// TestFeedEndToEnd runs the feed against a local server that behaves like the
// production one: it acknowledges each subscription, answers pings and streams
// recorded trades; the first connection is dropped to exercise resubscription.
func TestFeedEndToEnd(t *testing.T) {
	subs := make(chan string, 8)
	var pings sync.WaitGroup
	pings.Add(1)
	var pingOnce sync.Once
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		for range 2 { // one subscribe per symbol
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			subs <- string(msg)
			var req SubscribeRequest
			_ = json.Unmarshal(msg, &req)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"id":"`+req.Subscription.ID+`","name":"trade"}`))
		}
		for _, m := range []string{liveTradeBTC, `{"type":"trade","id":"BRLBTC","data":{"tid":"x"}}`, liveTradeUSDT} {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(m))
		}
		if n == 1 {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(liveTradeBTC2))
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if string(msg) == `{"type":"ping"}` {
				pingOnce.Do(pings.Done)
				_ = conn.WriteMessage(websocket.TextMessage, []byte(docPong))
			}
		}
	})

	var mu sync.Mutex
	decodeErrors := 0
	f, err := New(Config{URL: srv.URL("/ws"), Symbols: []string{"BTC-BRL", "USDT-BRL"}, Conn: testConn(), Logger: discard(),
		DecodeErrors: func() { mu.Lock(); decodeErrors++; mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Conn.PingInterval = 20 * time.Millisecond

	var events []app.Event
	enough := make(chan struct{})
	emit := func(_ context.Context, ev app.Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev)
		if len(events) == 7 {
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
	pingDone := make(chan struct{})
	go func() { pings.Wait(); close(pingDone) }()
	select {
	case <-pingDone:
	case <-time.After(5 * time.Second):
		t.Fatal("no application ping reached the server")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() = %v", err)
	}

	for i := range 4 {
		got := <-subs
		if want := []string{"BRLBTC", "BRLUSDT"}[i%2]; !strings.Contains(got, `"id":"`+want+`"`) || !strings.Contains(got, `"name":"trade"`) {
			t.Errorf("subscribe %d = %s", i+1, got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	var got []string
	for _, ev := range events {
		switch ev.Kind {
		case app.EventTrade:
			got = append(got, ev.Trade.Symbol)
		case app.EventConnected:
			got = append(got, "up")
		case app.EventDisconnected:
			got = append(got, "down")
		}
	}
	want := "up BTC-BRL USDT-BRL down up BTC-BRL USDT-BRL" // then the trades after pings, if any arrived yet
	if len(got) < 7 || strings.Join(got[:7], " ") != want {
		t.Fatalf("events = %v, want %s", got, want)
	}
	if decodeErrors < 2 {
		t.Errorf("malformed trade counted %d times, want once per connection", decodeErrors)
	}
}

func TestFeedStopsOnServerError(t *testing.T) {
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(liveBadPairErr))
		wstest.DrainUntilClosed(conn)
	})
	f, _ := New(Config{URL: srv.URL("/ws"), Symbols: []string{"NOPE-BRL"}, Conn: testConn(), Logger: discard()})
	err := f.Run(context.Background(), func(context.Context, app.Event) error { return nil })
	var perr *wsconn.PermanentError
	if !errors.As(err, &perr) || !strings.Contains(err.Error(), "invalid pair to subscribe") || !strings.Contains(err.Error(), "NOPE-BRL") {
		t.Fatalf("Run() = %v, want a permanent error naming the symbols", err)
	}
}

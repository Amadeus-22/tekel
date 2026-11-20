package wsfeed_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/feedtest"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn/wstest"
	"github.com/Amadeus-22/tekel/internal/adapter/wsfeed"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// feed is the smallest app.Feed over wsfeed.Run: each message's text is the
// event, "bad" is malformed and "stop" is a permanent failure.
type feed struct {
	url       string
	subscribe wsfeed.Subscribe
	bad       *wsconn.Malformed
}

func (f *feed) Exchange() string        { return "test" }
func (f *feed) Symbols() []string       { return []string{"X"} }
func (f *feed) Kind() domain.StreamKind { return domain.StreamTrades }
func (f *feed) Run(ctx context.Context, emit app.Emit) error {
	conn := feedtest.FastConn()
	conn.Name, conn.URL, conn.Logger = "test", f.url, discard
	return wsfeed.Run(ctx, conn, emit, f.subscribe, f.bad, func(msg []byte, at time.Time) ([]app.Event, error) {
		switch string(msg) {
		case "bad":
			return nil, errors.New("cannot decode")
		case "stop":
			return nil, wsconn.Permanent(errors.New("rejected"))
		case "none":
			return nil, nil
		}
		return []app.Event{{Kind: app.EventTrade, At: at, Trade: domain.Trade{Symbol: string(msg)}}}, nil
	})
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func newFeed(t *testing.T, script func(*websocket.Conn)) (*feed, *atomic.Int32) {
	t.Helper()
	srv := wstest.NewServer(t, func(_ int, c *websocket.Conn) { script(c) })
	var malformed atomic.Int32
	bad := wsconn.NewMalformed("test", discard, func() { malformed.Add(1) })
	return &feed{url: srv.URL("/"), bad: bad}, &malformed
}

func TestEventsInOrderAndConnectionStateAround(t *testing.T) {
	f, malformed := newFeed(t, func(c *websocket.Conn) {
		for _, m := range []string{"A", "none", "B"} {
			_ = c.WriteMessage(websocket.TextMessage, []byte(m))
		}
		wstest.DrainUntilClosed(c)
	})
	events, err := feedtest.Collect(t, f, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := feedtest.Kinds(events); got[0] != app.EventConnected || got[1] != app.EventTrade || got[2] != app.EventTrade {
		t.Fatalf("kinds = %v, want connected, trade, trade", got)
	}
	if events[1].Trade.Symbol != "A" || events[2].Trade.Symbol != "B" {
		t.Fatalf("trades out of order: %q, %q", events[1].Trade.Symbol, events[2].Trade.Symbol)
	}
	if malformed.Load() != 0 {
		t.Fatalf("malformed = %d, want 0", malformed.Load())
	}
}

func TestMalformedIsCountedAndSkippedWithoutDroppingTheConnection(t *testing.T) {
	f, malformed := newFeed(t, func(c *websocket.Conn) {
		for _, m := range []string{"bad", "A"} {
			_ = c.WriteMessage(websocket.TextMessage, []byte(m))
		}
		wstest.DrainUntilClosed(c)
	})
	events, err := feedtest.Collect(t, f, 2)
	if err != nil {
		t.Fatal(err)
	}
	if events[1].Kind != app.EventTrade || events[1].Trade.Symbol != "A" {
		t.Fatalf("event after the malformed one = %+v, want trade A on the same connection", events[1])
	}
	if malformed.Load() != 1 {
		t.Fatalf("malformed = %d, want 1", malformed.Load())
	}
}

func TestPermanentErrorStopsTheFeed(t *testing.T) {
	f, _ := newFeed(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, []byte("stop"))
		wstest.DrainUntilClosed(c)
	})
	_, err := feedtest.Collect(t, f, 10)
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("Run error = %v, want the permanent error", err)
	}
}

func TestSubscribeRunsAfterEveryDial(t *testing.T) {
	var got atomic.Value
	f, _ := newFeed(t, func(c *websocket.Conn) {
		_, msg, err := c.ReadMessage()
		if err == nil {
			got.Store(string(msg))
		}
		_ = c.WriteMessage(websocket.TextMessage, []byte("A"))
		wstest.DrainUntilClosed(c)
	})
	f.subscribe = func(_ context.Context, c *websocket.Conn) error {
		return c.WriteMessage(websocket.TextMessage, []byte("subscribe X"))
	}
	if _, err := feedtest.Collect(t, f, 2); err != nil {
		t.Fatal(err)
	}
	if got.Load() != "subscribe X" {
		t.Fatalf("server read %v, want the subscribe message", got.Load())
	}
}

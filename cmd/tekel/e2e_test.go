//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/wsconn/wstest"
)

// The end-to-end harness runs the whole service (config, fake Binance feed,
// pipeline, SQLite, HTTP API) in-process and checks what a user would see on
// /v1/incidents. Run it with `make test-integration`.
//
// To add a scenario: write a script for the fake exchange, call startTekel,
// then waitIncident for the kind you expect.

// binanceTrade is one message in Binance's combined-stream format.
func binanceTrade(id int64, price string) string {
	ms := time.Now().UnixMilli()
	return fmt.Sprintf(`{"stream":"btcusdt@trade","data":{"e":"trade","E":%d,"s":"BTCUSDT","t":%d,"p":%q,"q":"0.001","T":%d,"m":false}}`, ms, id, price, ms)
}

// send writes messages to the client in order.
func send(t *testing.T, conn *websocket.Conn, msgs ...string) {
	for _, m := range msgs {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(m)); err != nil {
			t.Errorf("fake exchange write: %v", err)
			return
		}
	}
}

// freeAddr returns a loopback address with a port that was free a moment ago.
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startTekel runs the service against the fake Binance server with every
// other feed off, and returns the base URL of its HTTP API. The service
// stops when the test ends.
func startTekel(t *testing.T, binance *wstest.Server) string {
	t.Helper()
	dir := t.TempDir()
	addr := freeAddr(t)
	env := map[string]string{
		"TEKEL_HTTP_ADDR":       addr,
		"TEKEL_DATA_DIR":        filepath.Join(dir, "data"),
		"TEKEL_DB_PATH":         filepath.Join(dir, "tekel.db"),
		"TEKEL_LOG_LEVEL":       "error",
		"TEKEL_BINANCE_URL":     binance.URL(""),
		"TEKEL_BINANCE_SYMBOLS": "BTCUSDT",
		"TEKEL_KRAKEN_SYMBOLS":  "",
	}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, lookup, io.Discard) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && ctx.Err() == nil {
				t.Errorf("run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("tekel did not stop within 10s")
		}
	})

	base := "http://" + addr
	eventually(t, func() bool {
		resp, err := http.Get(base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, "API never became healthy")
	return base
}

type incident struct {
	Exchange string `json:"exchange"`
	Symbol   string `json:"symbol"`
	Kind     string `json:"kind"`
	Detail   string `json:"detail"`
}

// incidents reads /v1/incidents with the given query.
func incidents(t *testing.T, base, query string) []incident {
	t.Helper()
	resp, err := http.Get(base + "/v1/incidents?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct{ Incidents []incident }
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode incidents: %v", err)
	}
	return body.Incidents
}

// waitIncident waits until the API lists an incident of kind for binance.
func waitIncident(t *testing.T, base, kind string) incident {
	t.Helper()
	var got incident
	eventually(t, func() bool {
		list := incidents(t, base, "exchange=binance&kind="+kind)
		if len(list) == 0 {
			return false
		}
		got = list[0]
		return true
	}, "no "+kind+" incident on /v1/incidents")
	return got
}

func eventually(t *testing.T, ok func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestE2ESequenceGapAndDuplicate(t *testing.T) {
	fake := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		// 3 is missing (gap) and 4 is sent twice (duplicate).
		send(t, conn,
			binanceTrade(1, "100.00"),
			binanceTrade(2, "100.01"),
			binanceTrade(4, "100.02"),
			binanceTrade(4, "100.02"),
		)
		wstest.DrainUntilClosed(conn)
	})
	base := startTekel(t, fake)

	gap := waitIncident(t, base, "sequence_gap")
	if gap.Symbol != "BTCUSDT" {
		t.Errorf("sequence_gap = %+v", gap)
	}
	waitIncident(t, base, "duplicate")
}

func TestE2EDisconnectIsRecordedAfterReconnect(t *testing.T) {
	fake := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		if n == 1 {
			send(t, conn, binanceTrade(1, "100.00"))
			return // drop the connection; the feed reconnects
		}
		send(t, conn, binanceTrade(2, "100.01"))
		wstest.DrainUntilClosed(conn)
	})
	base := startTekel(t, fake)

	waitIncident(t, base, "disconnect")
	if fake.Conns.Load() < 2 {
		t.Errorf("connections = %d, want a reconnect", fake.Conns.Load())
	}
}

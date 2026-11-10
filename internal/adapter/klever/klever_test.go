package klever

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tickguard/internal/adapter/feedtest"
	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn/wstest"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
	"github.com/Amadeus-22/tickguard/internal/domain/quality"
)

// Recorded from wss://node.testnet.klever.org/subscribe on 2026-11-10.
const liveBadTypeErr = `{"error":"invalid subscription type: bogus"}`

// recorded returns the three consecutive testnet blocks in testdata, verbatim
// (nonces 13844709..13844711, chainID "109").
func recorded(t *testing.T) []string {
	t.Helper()
	f, err := os.Open("testdata/testnet_blocks.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		out = append(out, sc.Text())
	}
	if err := sc.Err(); err != nil || len(out) != 3 {
		t.Fatalf("fixture: %d lines, %v", len(out), err)
	}
	return out
}

// withNonce rewrites a recorded block to nonce n, 4 s per nonce later.
func withNonce(t *testing.T, msg string, n int64) string {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal([]byte(msg), &env); err != nil {
		t.Fatal(err)
	}
	d := env["data"].(map[string]any)
	d["timestamp"] = d["timestamp"].(float64) + float64(n-int64(d["nonce"].(float64)))*4000
	d["nonce"] = n
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestDecode(t *testing.T) {
	recv := time.Date(2026, 11, 10, 17, 22, 20, 505_000_000, time.UTC)
	blocks := recorded(t)
	tests := []struct {
		name      string
		msg       string
		wantBlock bool
		wantErrMs string
		wantErr   bool
	}{
		{name: "live block", msg: blocks[0], wantBlock: true},
		{name: "live block with a transaction", msg: blocks[1], wantBlock: true},
		{name: "live handshake error", msg: liveBadTypeErr, wantErrMs: "invalid subscription type: bogus"},
		{name: "request reply ignored", msg: `{"id":"1","data":"subscribed"}`},
		{name: "transactions ignored", msg: `{"type":"transactions","address":"","hash":"","data":[]}`},
		{name: "not json", msg: `nope`, wantErr: true},
		{name: "data not an object", msg: `{"type":"blocks","data":[1]}`, wantErr: true},
		{name: "missing nonce", msg: `{"type":"blocks","data":{"hash":"ab","timestamp":1}}`, wantErr: true},
		{name: "negative nonce", msg: `{"type":"blocks","data":{"hash":"ab","nonce":-1,"timestamp":1}}`, wantErr: true},
		{name: "nonce out of range", msg: `{"type":"blocks","data":{"hash":"ab","nonce":18446744073709551615,"timestamp":1}}`, wantErr: true},
		{name: "missing timestamp", msg: `{"type":"blocks","data":{"hash":"ab","nonce":1}}`, wantErr: true},
		{name: "missing hash", msg: `{"type":"blocks","data":{"nonce":1,"timestamp":1}}`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Decode([]byte(tt.msg), recv, "testnet")
			if (err != nil) != tt.wantErr {
				t.Fatalf("Decode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if (m.Block != nil) != tt.wantBlock || m.Error != tt.wantErrMs {
				t.Fatalf("Decode() = %+v", m)
			}
		})
	}
}

func TestDecodeFields(t *testing.T) {
	recv := time.Date(2026, 11, 10, 17, 22, 24, 717_000_000, time.UTC)
	m, err := Decode([]byte(recorded(t)[1]), recv, "testnet")
	if err != nil {
		t.Fatal(err)
	}
	b := *m.Block
	want := domain.Block{
		Chain: "klever", Network: "testnet", Nonce: 13844710, TxCount: 1,
		Hash:      b.Hash, // checked below
		ChainTime: time.UnixMilli(1794331344000).UTC(), ReceivedTime: recv,
	}
	if b != want || len(b.Hash) != 64 || m.ChainID != "109" {
		t.Errorf("block = %+v chain %q\nwant    %+v chain \"109\"", b, m.ChainID, want)
	}
}

func TestSubscribeRequestShape(t *testing.T) {
	b, err := json.Marshal(NewSubscribeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"addresses":[],"subscribed_types":["blocks"]}`; string(b) != want {
		t.Errorf("subscribe = %s\nwant        %s", b, want)
	}
}

func TestNewValidates(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	for name, cfg := range map[string]Config{
		"no network":          {Logger: log},
		"unknown network":     {Network: "devnet", URL: "ws://x", Logger: log},
		"mainnet without url": {Network: "mainnet", Logger: log},
		"no logger":           {Network: "testnet"},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New() = nil error", name)
		}
	}
	f, err := New(Config{Network: "testnet", Logger: log, Conn: wsconn.DefaultConfig()})
	if err != nil {
		t.Fatal(err)
	}
	c := f.cfg.Conn
	if f.Exchange() != "klever" || f.Symbols()[0] != "testnet" || f.Kind() != domain.StreamBlocks ||
		c.URL != TestnetURL || c.ReadTimeout != defaultReadTimeout || c.MaxMessageBytes != maxMessageBytes || f.chainID != "109" {
		t.Errorf("feed = %s %v %s chain %s conn %+v", f.Exchange(), f.Symbols(), f.Kind(), f.chainID, c)
	}
	if _, err := New(Config{Network: "mainnet", URL: "wss://own-node.example/subscribe", Logger: log}); err != nil {
		t.Errorf("mainnet with url: %v", err)
	}
}

// TestFeedEndToEnd plays recorded testnet blocks through a local node: a normal
// run, a dropped connection, a replayed block and a block the node never sent.
// The emitted events then go through the block validator, the way the
// pipeline runs them, to show each incident comes out of real messages.
func TestFeedEndToEnd(t *testing.T) {
	blocks := recorded(t)
	skipped := withNonce(t, blocks[2], 13844714)
	subs := make(chan string, 2)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		subs <- string(msg)
		script := []string{blocks[0], `{"type":"blocks","data":{"nonce":"x"}}`, blocks[1]} // normal run
		if n > 1 {
			script = []string{
				blocks[1], // replayed after the reconnect: duplicate
				blocks[2], // first new block after the reconnect
				skipped,   // 712 and 713 never sent: silent gap
			}
		}
		for _, m := range script {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(m))
		}
		if n == 1 {
			return // drop the connection: a known gap
		}
		wstest.DrainUntilClosed(conn)
	})

	decodeErrors := 0
	var mu sync.Mutex
	f, err := New(Config{URL: srv.URL("/subscribe"), Network: "testnet", Conn: feedtest.FastConn(), Logger: slog.New(slog.DiscardHandler),
		DecodeErrors: func() { mu.Lock(); decodeErrors++; mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	events, err := feedtest.Collect(t, f, 8)
	if err != nil {
		t.Fatalf("Run() = %v", err)
	}
	for range 2 {
		if got := strings.TrimSpace(<-subs); got != `{"addresses":[],"subscribed_types":["blocks"]}` {
			t.Errorf("subscribe = %s", got)
		}
	}
	mu.Lock()
	if decodeErrors != 1 {
		t.Errorf("decode errors = %d, want 1", decodeErrors)
	}
	mu.Unlock()

	// Replay through the validator. The fixtures are older than the test run,
	// so clock skew is out of scope here (quality tests cover it).
	cfg := quality.DefaultConfig()
	cfg.ClockSkew = 100 * 365 * 24 * time.Hour
	s := quality.NewBlockStream(domain.StreamKey{Exchange: "klever", Symbol: "testnet"}, cfg)
	var trace, incidents []string
	var last time.Time
	for _, ev := range events {
		switch ev.Kind {
		case app.EventConnected:
			trace = append(trace, "up")
			if inc, ok := s.Connected(ev.At); ok {
				incidents = append(incidents, string(inc.Kind))
			}
		case app.EventDisconnected:
			trace = append(trace, "down")
			s.Disconnected(ev.At)
		case app.EventBlock:
			trace = append(trace, strconv.FormatInt(ev.Block.Nonce-13844000, 10))
			for _, inc := range s.ObserveBlock(ev.Block).Incidents {
				incidents = append(incidents, string(inc.Kind)+":"+inc.Detail)
			}
			last = ev.Block.ReceivedTime
		}
	}
	if got, want := strings.Join(trace, " "), "up 709 710 down up 710 711 714"; got != want {
		t.Errorf("events = %s, want %s", got, want)
	}
	want := []string{
		"disconnect",
		"duplicate:nonce 13844710 already seen in the last 10000 blocks",
		"sequence_gap:nonce jumped from 13844711 to 13844714 while connected (2 missing)",
	}
	if strings.Join(incidents, "|") != strings.Join(want, "|") {
		t.Errorf("incidents = %q\nwant        %q", incidents, want)
	}
	if inc, ok := s.CheckStale(last.Add(61 * time.Second)); !ok || inc.Kind != domain.KindStaleStream {
		t.Errorf("no block for 61 s: %+v %v, want stale_stream", inc, ok)
	}
}

func TestFeedStopsOnNodeError(t *testing.T) {
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(liveBadTypeErr))
		wstest.DrainUntilClosed(conn)
	})
	f, _ := New(Config{URL: srv.URL("/subscribe"), Network: "testnet", Conn: feedtest.FastConn(), Logger: slog.New(slog.DiscardHandler)})
	_, err := feedtest.Collect(t, f, 99)
	var perr *wsconn.PermanentError
	if !errors.As(err, &perr) || !strings.Contains(err.Error(), "invalid subscription type") {
		t.Fatalf("Run() = %v, want a permanent error with the node's message", err)
	}
}

// TestFeedStopsOnWrongChain: testnet blocks on a feed configured as mainnet
// must not be recorded as mainnet.
func TestFeedStopsOnWrongChain(t *testing.T) {
	block := recorded(t)[0]
	srv := wstest.NewServer(t, func(_ int, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(block))
		wstest.DrainUntilClosed(conn)
	})
	f, _ := New(Config{URL: srv.URL("/subscribe"), Network: "mainnet", Conn: feedtest.FastConn(), Logger: slog.New(slog.DiscardHandler)})
	events, err := feedtest.Collect(t, f, 99)
	var perr *wsconn.PermanentError
	if !errors.As(err, &perr) || !strings.Contains(err.Error(), `chain "109"`) {
		t.Fatalf("Run() = %v, want a permanent chain mismatch", err)
	}
	for _, ev := range events {
		if ev.Kind == app.EventBlock {
			t.Errorf("emitted a block from the wrong chain: %+v", ev.Block)
		}
	}
}

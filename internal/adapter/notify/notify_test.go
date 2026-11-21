package notify

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

type memStore struct {
	mu    sync.Mutex
	saved []domain.Incident
	err   error
}

func (m *memStore) Save(_ context.Context, inc domain.Incident) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saved = append(m.saved, inc)
	return m.err
}

type counter struct {
	mu sync.Mutex
	n  map[string]int
}

func (c *counter) count(sender, outcome string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n == nil {
		c.n = map[string]int{}
	}
	c.n[sender+"/"+outcome]++
}

func (c *counter) get(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n[key]
}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func gap(symbol string) domain.Incident {
	t := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	return domain.Incident{Exchange: "binance", Symbol: symbol, Kind: domain.KindSequenceGap,
		DetectedAt: t, Start: t.Add(-2 * time.Second), End: t, Detail: "3 trades missing after id 100"}
}

func receiver(t *testing.T, status int, delay time.Duration) (*httptest.Server, <-chan map[string]any) {
	t.Helper()
	got := make(chan map[string]any, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(delay)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		body["_path"] = r.URL.Path
		got <- body
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func start(t *testing.T, s *Store) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run: %v", err)
		}
	})
}

func wait(t *testing.T, ch <-chan map[string]any) map[string]any {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("no message received")
		return nil
	}
}

func newStore(t *testing.T, next *memStore, c *counter, senders ...Sender) *Store {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Logger, cfg.Count, cfg.Timeout = discard, c.count, 200*time.Millisecond
	s, err := New(next, senders, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestWebhookPayloadByHost(t *testing.T) {
	srv, got := receiver(t, http.StatusOK, 0)
	text := Message(gap("BTCUSDT"))
	for _, tc := range []struct{ name, field string }{
		{"hooks.slack.com", "text"}, {"discord.com", "content"}, {"example.org", "text"},
	} {
		w := Webhook{URL: "https://" + tc.name + "/hook", Client: redirectTo(srv.URL)}
		if err := w.Send(context.Background(), gap("BTCUSDT"), text); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		body := wait(t, got)
		if body[tc.field] != text {
			t.Errorf("%s: %s = %v, want the one-line message", tc.name, tc.field, body[tc.field])
		}
		if _, ok := body["incident"]; ok != (tc.name == "example.org") {
			t.Errorf("%s: structured incident present = %v", tc.name, ok)
		}
	}
}

func TestTelegramSendsToTheBotChatWithoutLeakingTheToken(t *testing.T) {
	srv, got := receiver(t, http.StatusOK, 0)
	tg := Telegram{Token: "123:SECRET", ChatID: "-100", API: srv.URL}
	if err := tg.Send(context.Background(), gap("X"), "hello"); err != nil {
		t.Fatal(err)
	}
	body := wait(t, got)
	if body["_path"] != "/bot123:SECRET/sendMessage" || body["chat_id"] != "-100" || body["text"] != "hello" {
		t.Fatalf("telegram request = %v", body)
	}
	// An unreachable API must not echo the URL (and so the token) in the error.
	tg.API = "http://127.0.0.1:1"
	err := tg.Send(context.Background(), gap("X"), "hello")
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error = %v, want a failure that does not contain the token", err)
	}
}

func TestOnlyChosenKindsAreNotifiedAndEverythingIsSaved(t *testing.T) {
	srv, got := receiver(t, http.StatusOK, 0)
	next, c := &memStore{}, &counter{}
	s := newStore(t, next, c, Webhook{URL: srv.URL})
	start(t, s)

	dup := gap("BTCUSDT")
	dup.Kind = domain.KindDuplicate
	_ = s.Save(context.Background(), dup)
	_ = s.Save(context.Background(), gap("BTCUSDT"))

	if body := wait(t, got); !strings.Contains(body["text"].(string), "sequence_gap") {
		t.Fatalf("notified %v, want the gap", body["text"])
	}
	select {
	case b := <-got:
		t.Fatalf("duplicate was notified too: %v", b)
	case <-time.After(100 * time.Millisecond):
	}
	if len(next.saved) != 2 {
		t.Fatalf("saved %d incidents, want both", len(next.saved))
	}
}

func TestRateLimitPerStreamAndKind(t *testing.T) {
	srv, got := receiver(t, http.StatusOK, 0)
	next, c := &memStore{}, &counter{}
	s := newStore(t, next, c, Webhook{URL: srv.URL})
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	s.cfg.Now = func() time.Time { return now }
	start(t, s)

	_ = s.Save(context.Background(), gap("BTCUSDT"))
	_ = s.Save(context.Background(), gap("BTCUSDT")) // same stream, same minute
	_ = s.Save(context.Background(), gap("ETHUSDT")) // other stream
	wait(t, got)
	wait(t, got)
	now = now.Add(time.Minute)
	_ = s.Save(context.Background(), gap("BTCUSDT")) // interval elapsed
	wait(t, got)
	if c.get("/limited") != 1 {
		t.Fatalf("limited = %d, want 1", c.get("/limited"))
	}
}

func TestFailingEndpointKeepsTheIncidentAndCountsTheFailure(t *testing.T) {
	srv, got := receiver(t, http.StatusInternalServerError, 0)
	next, c := &memStore{err: errors.New("disk full")}, &counter{}
	s := newStore(t, next, c, Webhook{URL: srv.URL})
	start(t, s)

	if err := s.Save(context.Background(), gap("BTCUSDT")); err == nil || err.Error() != "disk full" {
		t.Fatalf("Save error = %v, want the store's own error passed through", err)
	}
	wait(t, got)
	deadline := time.Now().Add(2 * time.Second)
	for c.get("webhook/failed") == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.get("webhook/failed") != 1 || len(next.saved) != 1 {
		t.Fatalf("failed = %d, saved = %d", c.get("webhook/failed"), len(next.saved))
	}
}

func TestSlowEndpointNeverBlocksSave(t *testing.T) {
	srv, _ := receiver(t, http.StatusOK, time.Second)
	next, c := &memStore{}, &counter{}
	cfg := DefaultConfig()
	cfg.Logger, cfg.Count, cfg.Queue, cfg.Interval = discard, c.count, 1, 0
	s, err := New(next, []Sender{Webhook{URL: srv.URL}}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	start(t, s)

	began := time.Now()
	for i := 0; i < 20; i++ {
		_ = s.Save(context.Background(), gap("BTCUSDT"))
	}
	if took := time.Since(began); took > 200*time.Millisecond {
		t.Fatalf("20 saves took %v with a slow endpoint, want no blocking", took)
	}
	if len(next.saved) != 20 || c.get("/dropped") == 0 {
		t.Fatalf("saved %d, dropped %d: want every incident saved and notifications dropped", len(next.saved), c.get("/dropped"))
	}
}

func TestMessage(t *testing.T) {
	want := "tekel: binance BTCUSDT sequence_gap — 3 trades missing after id 100 (2026-11-21 11:59:58 → 12:00:00 UTC)"
	if got := Message(gap("BTCUSDT")); got != want {
		t.Fatalf("Message =\n%s\nwant\n%s", got, want)
	}
}

// redirectTo sends every request to base, keeping the path: lets the
// Slack and Discord host rules be tested against a local server.
func redirectTo(base string) *http.Client {
	return &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		r2 := r.Clone(r.Context())
		r2.URL.Scheme, r2.URL.Host = "http", strings.TrimPrefix(base, "http://")
		r2.Host = r2.URL.Host
		return http.DefaultTransport.RoundTrip(r2)
	})}
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

package wsconn

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn/wstest"
)

func testConfig(url string) Config {
	cfg := DefaultConfig()
	cfg.Name = "test"
	cfg.URL = url
	cfg.DialTimeout = time.Second
	cfg.ReadTimeout = time.Second
	cfg.WriteTimeout = time.Second
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = 5 * time.Millisecond
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return cfg
}

// recorder captures callbacks and lets the test wait for them.
type recorder struct {
	mu    sync.Mutex
	ups   int
	downs []error
	msgs  []string
	subs  int
	ch    chan string
}

func newRecorder() *recorder { return &recorder{ch: make(chan string, 100)} }

func (r *recorder) handlers() Handlers {
	return Handlers{
		Subscribe: func(_ context.Context, c *websocket.Conn) error {
			r.mu.Lock()
			r.subs++
			r.mu.Unlock()
			return c.WriteMessage(websocket.TextMessage, []byte("subscribe"))
		},
		Up: func(context.Context, time.Time) error {
			r.mu.Lock()
			r.ups++
			r.mu.Unlock()
			r.ch <- "up"
			return nil
		},
		Message: func(_ context.Context, msg []byte, received time.Time) error {
			if received.IsZero() {
				return errors.New("zero receive time")
			}
			r.mu.Lock()
			r.msgs = append(r.msgs, string(msg))
			r.mu.Unlock()
			r.ch <- "msg:" + string(msg)
			return nil
		},
		Down: func(_ context.Context, _ time.Time, cause error) error {
			r.mu.Lock()
			r.downs = append(r.downs, cause)
			r.mu.Unlock()
			r.ch <- "down"
			return nil
		},
	}
}

func (r *recorder) waitFor(t *testing.T, want string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case got := <-r.ch:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}

func run(t *testing.T, cfg Config, h Handlers) (cancel func(), done <-chan error) {
	t.Helper()
	ctx, c := context.WithCancel(context.Background())
	ch := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		ch <- Run(ctx, cfg, h)
	}()
	t.Cleanup(func() {
		c()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return c, ch
}

func TestRunReconnectsAndResubscribes(t *testing.T) {
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		if _, msg, err := conn.ReadMessage(); err != nil || string(msg) != "subscribe" {
			t.Errorf("conn %d: first message = %q, %v", n, msg, err)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte("hello"))
		if n == 1 {
			return // drop the first connection abruptly
		}
		wstest.DrainUntilClosed(conn)
	})
	rec := newRecorder()
	run(t, testConfig(srv.URL("/")), rec.handlers())

	rec.waitFor(t, "up")
	rec.waitFor(t, "msg:hello")
	rec.waitFor(t, "down")
	rec.waitFor(t, "up")
	rec.waitFor(t, "msg:hello")

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.subs != 2 || rec.ups != 2 || len(rec.downs) != 1 {
		t.Errorf("subs=%d ups=%d downs=%d, want 2/2/1", rec.subs, rec.ups, len(rec.downs))
	}
	if rec.downs[0] == nil {
		t.Error("Down cause is nil")
	}
}

func TestRunReadTimeoutReconnects(t *testing.T) {
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		wstest.DrainUntilClosed(conn) // never sends anything
	})
	cfg := testConfig(srv.URL("/"))
	cfg.ReadTimeout = 50 * time.Millisecond
	rec := newRecorder()
	h := rec.handlers()
	h.Subscribe = nil
	run(t, cfg, h)

	rec.waitFor(t, "down")
	rec.mu.Lock()
	cause := rec.downs[0]
	rec.mu.Unlock()
	if !strings.Contains(cause.Error(), "timeout") {
		t.Errorf("cause = %v, want a read timeout", cause)
	}
	rec.waitFor(t, "up")
}

func TestRunClientPingsKeepConnectionAlive(t *testing.T) {
	pinged := make(chan struct{}, 10)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		conn.SetPingHandler(func(data string) error {
			pinged <- struct{}{}
			return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
		})
		wstest.DrainUntilClosed(conn)
	})
	cfg := testConfig(srv.URL("/"))
	cfg.PingInterval = 20 * time.Millisecond
	cfg.ReadTimeout = 200 * time.Millisecond // only pongs keep it open
	rec := newRecorder()
	h := rec.handlers()
	h.Subscribe = nil
	run(t, cfg, h)

	rec.waitFor(t, "up")
	for i := 0; i < 20; i++ { // ~400ms of pings, twice the read timeout
		select {
		case <-pinged:
		case <-time.After(time.Second):
			t.Fatal("no ping received")
		}
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.downs) != 0 {
		t.Errorf("connection dropped despite pongs: %v", rec.downs)
	}
}

func TestRunAppPingAndHeader(t *testing.T) {
	pings := make(chan string, 10)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			pings <- string(msg)
			// the protocol-level reply is what keeps the read deadline alive
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"pong"}`)); err != nil {
				return
			}
		}
	})
	cfg := testConfig(srv.URL("/"))
	cfg.PingInterval = 20 * time.Millisecond
	cfg.ReadTimeout = 200 * time.Millisecond
	cfg.AppPing = []byte(`{"type":"ping"}`)
	cfg.Header = http.Header{"User-Agent": {"tickguard-test"}}
	rec := newRecorder()
	h := rec.handlers()
	h.Subscribe = nil
	run(t, cfg, h)

	rec.waitFor(t, "up")
	for i := 0; i < 20; i++ { // ~400ms, twice the read timeout
		select {
		case got := <-pings:
			if got != `{"type":"ping"}` {
				t.Fatalf("ping = %q", got)
			}
		case <-time.After(time.Second):
			t.Fatal("no application ping received")
		}
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.downs) != 0 {
		t.Errorf("connection dropped despite pong messages: %v", rec.downs)
	}
	if ua := srv.Header().Get("User-Agent"); ua != "tickguard-test" {
		t.Errorf("User-Agent = %q", ua)
	}
}

func TestRunAnswersServerPings(t *testing.T) {
	ponged := make(chan string, 1)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		conn.SetPongHandler(func(data string) error { ponged <- data; return nil })
		_ = conn.WriteControl(websocket.PingMessage, []byte("p1"), time.Now().Add(time.Second))
		wstest.DrainUntilClosed(conn)
	})
	rec := newRecorder()
	h := rec.handlers()
	h.Subscribe = nil
	run(t, testConfig(srv.URL("/")), h)
	select {
	case got := <-ponged:
		if got != "p1" {
			t.Errorf("pong payload = %q, want the ping's payload", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server never got a pong")
	}
}

func TestRunCancelClosesCleanly(t *testing.T) {
	closeCode := make(chan int, 1)
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		_, _, err := conn.ReadMessage()
		var ce *websocket.CloseError
		if errors.As(err, &ce) {
			closeCode <- ce.Code
		}
	})
	rec := newRecorder()
	h := rec.handlers()
	h.Subscribe = nil
	cancel, done := run(t, testConfig(srv.URL("/")), h)
	rec.waitFor(t, "up")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run() = %v, want nil on cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	select {
	case code := <-closeCode:
		if code != websocket.CloseNormalClosure {
			t.Errorf("close code = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not receive a close frame")
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.downs) != 0 {
		t.Error("Down must not be called for a shutdown")
	}
}

func TestRunRetriesDialFailures(t *testing.T) {
	srv := wstest.NewServer(t, func(int, *websocket.Conn) {})
	url := srv.URL("/")
	srv.Close() // nothing listens any more

	rec := newRecorder()
	cancel, done := run(t, testConfig(url), rec.handlers())
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() = %v", err)
	}
	if rec.ups != 0 {
		t.Error("Up called without a connection")
	}
}

func TestRunStopsOnPermanentError(t *testing.T) {
	srv := wstest.NewServer(t, func(n int, conn *websocket.Conn) {
		_ = conn.WriteMessage(websocket.TextMessage, []byte("rejected"))
		wstest.DrainUntilClosed(conn)
	})
	h := Handlers{Message: func(context.Context, []byte, time.Time) error {
		return Permanent(errors.New("subscription rejected"))
	}}
	_, done := run(t, testConfig(srv.URL("/")), h)
	select {
	case err := <-done:
		var perr *PermanentError
		if !errors.As(err, &perr) || !strings.Contains(err.Error(), "test: subscription rejected") {
			t.Errorf("Run() = %v, want the permanent error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run kept retrying a permanent error")
	}
	if srv.Conns.Load() != 1 {
		t.Errorf("connections = %d, want 1", srv.Conns.Load())
	}
}

func TestRunValidates(t *testing.T) {
	ok := testConfig("ws://127.0.0.1:1/")
	h := Handlers{Message: func(context.Context, []byte, time.Time) error { return nil }}
	tests := []struct {
		name string
		cfg  func(*Config)
		h    Handlers
	}{
		{"url", func(c *Config) { c.URL = "" }, h},
		{"logger", func(c *Config) { c.Logger = nil }, h},
		{"handler", func(*Config) {}, Handlers{}},
		{"timeouts", func(c *Config) { c.ReadTimeout = 0 }, h},
		{"backoff", func(c *Config) { c.BackoffMax = 0 }, h},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := ok
			tt.cfg(&cfg)
			if err := Run(context.Background(), cfg, tt.h); err == nil {
				t.Error("Run() = nil, want a validation error")
			}
		})
	}
}

func TestBackoff(t *testing.T) {
	lo, hi := 500*time.Millisecond, 30*time.Second
	zero := func() float64 { return 0 }
	almostOne := func() float64 { return 0.999999 }
	tests := []struct {
		attempt  int
		rnd      func() float64
		min, max time.Duration
	}{
		{0, zero, 250 * time.Millisecond, 250 * time.Millisecond},
		{0, almostOne, 499 * time.Millisecond, 500 * time.Millisecond},
		{1, zero, 500 * time.Millisecond, 500 * time.Millisecond},
		{3, almostOne, 3999 * time.Millisecond, 4 * time.Second},
		{6, zero, 15 * time.Second, 15 * time.Second}, // 32s capped to 30s
		{6, almostOne, 29999 * time.Millisecond, 30 * time.Second},
		{100, zero, 15 * time.Second, 15 * time.Second}, // no overflow
	}
	for _, tt := range tests {
		got := Backoff(tt.attempt, lo, hi, tt.rnd)
		if got < tt.min || got > tt.max {
			t.Errorf("Backoff(%d) = %v, want in [%v, %v]", tt.attempt, got, tt.min, tt.max)
		}
	}
}

func TestThrottle(t *testing.T) {
	th := NewThrottle(time.Minute)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	steps := []struct {
		at         time.Duration
		ok         bool
		suppressed int
	}{
		{0, true, 0},
		{time.Second, false, 0},
		{59 * time.Second, false, 0},
		{time.Minute, true, 2},
		{time.Minute + time.Second, false, 0},
		{3 * time.Minute, true, 1},
	}
	for _, s := range steps {
		ok, n := th.Allow(now.Add(s.at))
		if ok != s.ok || n != s.suppressed {
			t.Errorf("Allow(+%v) = %v, %d; want %v, %d", s.at, ok, n, s.ok, s.suppressed)
		}
	}
}

func TestMalformedCountsEveryMessageAndLogsASample(t *testing.T) {
	var logs strings.Builder
	count := 0
	m := NewMalformed("test", slog.New(slog.NewTextHandler(&logs, nil)), func() { count++ })
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	long := strings.Repeat("x", 300)
	m.Report([]byte(long), now, errors.New("bad"))
	m.Report([]byte("second"), now.Add(time.Second), errors.New("bad"))
	if count != 2 {
		t.Errorf("counted %d, want 2", count)
	}
	out := logs.String()
	if strings.Count(out, "malformed message skipped") != 1 || !strings.Contains(out, strings.Repeat("x", 200)+"…") ||
		strings.Contains(out, strings.Repeat("x", 201)) || strings.Contains(out, "second") {
		t.Errorf("logs = %s", out)
	}
	NewMalformed("test", slog.New(slog.DiscardHandler), nil).Report(nil, now, errors.New("nil count is allowed"))
}

func TestPermanentErrorUnwraps(t *testing.T) {
	base := errors.New("base")
	if !errors.Is(Permanent(base), base) {
		t.Error("Permanent does not unwrap")
	}
}

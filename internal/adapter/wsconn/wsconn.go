// Package wsconn is the WebSocket connection loop shared by the exchange
// adapters: dial with a timeout, (re)subscribe, keep the connection alive with
// ping/pong and a read deadline, and reconnect with capped, jittered backoff.
package wsconn

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Config describes one logical connection.
type Config struct {
	// Name identifies the connection in logs, e.g. "binance".
	Name string
	URL  string

	// DialTimeout bounds the TCP + TLS + WebSocket handshake.
	DialTimeout time.Duration
	// ReadTimeout is the longest silence tolerated before the connection is
	// considered dead. Any message, ping or pong resets it.
	ReadTimeout time.Duration
	// WriteTimeout bounds every write (subscribe, pong, ping, close).
	WriteTimeout time.Duration
	// PingInterval sends a client ping this often; zero disables client pings
	// (useful when the server pings us, as Binance does).
	PingInterval time.Duration
	// BackoffMin and BackoffMax bound the delay between reconnect attempts.
	BackoffMin time.Duration
	BackoffMax time.Duration
	// MaxMessageBytes caps one inbound message.
	MaxMessageBytes int64

	Logger *slog.Logger
}

// Handlers are the callbacks of one Run. Any of them may return an error to end
// the current connection; wrap it with Permanent to stop Run altogether.
type Handlers struct {
	// Subscribe runs after every successful dial, before Up. Nil means the
	// subscription is encoded in the URL.
	Subscribe func(ctx context.Context, conn *websocket.Conn) error
	// Up is called once the connection is subscribed.
	Up func(ctx context.Context, at time.Time) error
	// Message is called for every data message with the time it was read.
	Message func(ctx context.Context, msg []byte, received time.Time) error
	// Down is called when a connection that was Up ends, with the cause.
	// It is not called when the connection ends because ctx was cancelled.
	Down func(ctx context.Context, at time.Time, cause error) error
}

// DefaultConfig returns conservative timeouts; callers set Name and URL.
func DefaultConfig() Config {
	return Config{
		DialTimeout:     10 * time.Second,
		ReadTimeout:     60 * time.Second,
		WriteTimeout:    5 * time.Second,
		PingInterval:    0,
		BackoffMin:      500 * time.Millisecond,
		BackoffMax:      30 * time.Second,
		MaxMessageBytes: 1 << 20,
	}
}

// PermanentError marks an error that reconnecting cannot fix, such as a
// subscription the exchange rejected. Run returns it instead of retrying.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so that Run stops instead of reconnecting.
func Permanent(err error) error { return &PermanentError{Err: err} }

// Run keeps a connection up until ctx is cancelled (returns nil) or a handler
// returns a Permanent error (returns it). Every other failure is retried.
func Run(ctx context.Context, cfg Config, h Handlers) error {
	if err := cfg.validate(h); err != nil {
		return err
	}
	attempt := 0
	for {
		healthy, err := session(ctx, cfg, h)
		if ctx.Err() != nil {
			return nil
		}
		var perr *PermanentError
		if errors.As(err, &perr) {
			return fmt.Errorf("%s: %w", cfg.Name, err)
		}
		if healthy {
			attempt = 0 // the connection worked; start backing off from scratch
		}
		delay := Backoff(attempt, cfg.BackoffMin, cfg.BackoffMax, rand.Float64)
		attempt++
		cfg.Logger.Warn("websocket connection ended, reconnecting",
			"conn", cfg.Name, "error", err.Error(), "attempt", attempt, "retry_in", delay.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}

func (c Config) validate(h Handlers) error {
	switch {
	case c.URL == "":
		return errors.New("wsconn: URL is required")
	case c.Logger == nil:
		return errors.New("wsconn: logger is required")
	case h.Message == nil:
		return errors.New("wsconn: message handler is required")
	case c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0:
		return errors.New("wsconn: dial, read and write timeouts must be positive")
	case c.BackoffMin <= 0 || c.BackoffMax < c.BackoffMin:
		return errors.New("wsconn: backoff bounds must satisfy 0 < min <= max")
	}
	return nil
}

// session runs one connection from dial to failure. healthy reports whether it
// got as far as reading a message, which resets the backoff.
func session(ctx context.Context, cfg Config, h Handlers) (healthy bool, err error) {
	dialer := websocket.Dialer{HandshakeTimeout: cfg.DialTimeout, Proxy: http.ProxyFromEnvironment}
	dialCtx, cancel := context.WithTimeout(ctx, cfg.DialTimeout)
	conn, resp, err := dialer.DialContext(dialCtx, cfg.URL, nil)
	cancel()
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", cfg.URL, err)
	}
	defer conn.Close()

	if cfg.MaxMessageBytes > 0 {
		conn.SetReadLimit(cfg.MaxMessageBytes)
	}
	extend := func() error { return conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)) }
	conn.SetPongHandler(func(string) error { return extend() })
	conn.SetPingHandler(func(data string) error {
		_ = extend()
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(cfg.WriteTimeout))
		if errors.Is(err, websocket.ErrCloseSent) {
			return nil
		}
		return err
	})
	if err := extend(); err != nil {
		return false, fmt.Errorf("set read deadline: %w", err)
	}

	if h.Subscribe != nil {
		if err := conn.SetWriteDeadline(time.Now().Add(cfg.WriteTimeout)); err != nil {
			return false, fmt.Errorf("set write deadline: %w", err)
		}
		if err := h.Subscribe(ctx, conn); err != nil {
			return false, fmt.Errorf("subscribe: %w", err)
		}
	}
	if h.Up != nil {
		if err := h.Up(ctx, time.Now()); err != nil {
			return false, err
		}
	}

	// The keeper owns pings and closes the socket when ctx ends, which unblocks
	// ReadMessage below. It exits before session returns.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		keep(ctx, cfg, conn, stop)
	}()

	cause := readLoop(ctx, conn, h, extend, &healthy)
	close(stop)
	wg.Wait()

	if ctx.Err() != nil {
		return healthy, ctx.Err()
	}
	if h.Down != nil {
		if err := h.Down(ctx, time.Now(), cause); err != nil {
			return healthy, err
		}
	}
	return healthy, cause
}

func readLoop(ctx context.Context, conn *websocket.Conn, h Handlers, extend func() error, healthy *bool) error {
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		received := time.Now()
		*healthy = true
		if err := extend(); err != nil {
			return fmt.Errorf("set read deadline: %w", err)
		}
		if err := h.Message(ctx, msg, received); err != nil {
			return err
		}
	}
}

func keep(ctx context.Context, cfg Config, conn *websocket.Conn, stop <-chan struct{}) {
	var tick <-chan time.Time
	if cfg.PingInterval > 0 {
		t := time.NewTicker(cfg.PingInterval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			msg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down")
			_ = conn.WriteControl(websocket.CloseMessage, msg, time.Now().Add(cfg.WriteTimeout))
			_ = conn.Close()
			return
		case <-tick:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(cfg.WriteTimeout)); err != nil {
				_ = conn.Close() // the read loop will report why
				return
			}
		}
	}
}

// Backoff returns the delay before reconnect attempt n (0-based): exponential
// from lo, capped at hi, with "equal jitter" — half fixed, half random — so
// that many clients dropped at once do not reconnect in lockstep, yet no retry
// is ever immediate. rnd returns a value in [0, 1).
func Backoff(attempt int, lo, hi time.Duration, rnd func() float64) time.Duration {
	d := hi
	if attempt < 32 {
		if exp := lo << attempt; exp > 0 && exp < hi {
			d = exp
		}
	}
	half := d / 2
	return half + time.Duration(rnd()*float64(d-half))
}

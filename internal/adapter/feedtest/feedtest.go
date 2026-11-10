// Package feedtest holds test helpers shared by the feed adapters: a fast
// reconnecting connection config and a collector that runs a feed until it
// has emitted enough events. It lives outside wsconn/wstest because it
// imports app, and wsconn's own tests import wstest.
package feedtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/app"
)

// FastConn returns wsconn.DefaultConfig with millisecond reconnect backoff.
func FastConn() wsconn.Config {
	c := wsconn.DefaultConfig()
	c.BackoffMin, c.BackoffMax = time.Millisecond, 5*time.Millisecond
	return c
}

// Collect runs f until it has emitted n events or Run returns, then cancels
// it and returns the events and Run's result. It fails the test after 5 s.
func Collect(t testing.TB, f app.Feed, n int) ([]app.Event, error) {
	t.Helper()
	var mu sync.Mutex
	var events []app.Event
	enough := make(chan struct{})
	emit := func(_ context.Context, ev app.Event) error {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev)
		if len(events) == n {
			close(enough)
		}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx, emit) }()
	var err error
	select {
	case <-enough:
		cancel()
		err = <-done
	case err = <-done:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatalf("feed emitted %d events in 5 s, want %d", len(events), n)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]app.Event(nil), events...), err
}

// Kinds returns the kind of every event, for compact assertions.
func Kinds(events []app.Event) []app.EventKind {
	out := make([]app.EventKind, len(events))
	for i, ev := range events {
		out[i] = ev.Kind
	}
	return out
}

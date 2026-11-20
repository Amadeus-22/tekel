// Package wsfeed is the part every websocket exchange feed shares: connection
// state becomes app events, and a message that cannot be read is counted,
// logged (throttled) and skipped. An adapter only says how to subscribe and
// what one message means.
package wsfeed

import (
	"context"
	"errors"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/app"
)

// Subscribe sends the subscription after each dial. Nil means the URL alone
// subscribes (Binance's combined streams).
type Subscribe func(ctx context.Context, conn *websocket.Conn) error

// Handle turns one message into the events it carries, in order; none is fine
// (heartbeats, acknowledgements). Return an error wrapped with
// wsconn.Permanent to stop the feed, for a failure that reconnecting cannot
// fix. Any other error marks the message malformed: it is reported to bad
// and skipped, and the connection stays up.
type Handle func(msg []byte, received time.Time) ([]app.Event, error)

// Run connects with conn and emits until ctx ends or handle returns a
// permanent error.
func Run(ctx context.Context, conn wsconn.Config, emit app.Emit, subscribe Subscribe, bad *wsconn.Malformed, handle Handle) error {
	return wsconn.Run(ctx, conn, wsconn.Handlers{
		Subscribe: subscribe,
		Up: func(ctx context.Context, at time.Time) error {
			return emit(ctx, app.Event{Kind: app.EventConnected, At: at})
		},
		Down: func(ctx context.Context, at time.Time, cause error) error {
			return emit(ctx, app.Event{Kind: app.EventDisconnected, At: at, Err: cause})
		},
		Message: func(ctx context.Context, msg []byte, received time.Time) error {
			events, err := handle(msg, received)
			if err != nil {
				var perr *wsconn.PermanentError
				if errors.As(err, &perr) {
					return err
				}
				bad.Report(msg, received, err)
				return nil
			}
			for _, ev := range events {
				if err := emit(ctx, ev); err != nil {
					return err
				}
			}
			return nil
		},
	})
}

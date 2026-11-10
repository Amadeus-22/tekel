// Package app holds Tekel's use case: the Pipeline that fans in exchange
// feeds, validates every trade and routes the results to the sinks. It defines
// the ports (interfaces) it needs; adapters implement them.
package app

import (
	"context"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

// EventKind says what an Event carries.
type EventKind int

// Event kinds.
const (
	// EventTrade carries a decoded trade.
	EventTrade EventKind = iota + 1
	// EventConnected means the feed is connected and subscribed.
	EventConnected
	// EventDisconnected means the feed lost its connection.
	EventDisconnected
	// EventBlock carries a decoded chain block (feeds of kind StreamBlocks).
	EventBlock
)

// Event is what a Feed emits. Connection events apply to every symbol of the feed.
type Event struct {
	Kind EventKind
	// Exchange is set by the pipeline from Feed.Exchange; feeds leave it empty.
	Exchange string
	// At is when the event happened on Tekel's clock.
	At    time.Time
	Trade domain.Trade // EventTrade only
	Block domain.Block // EventBlock only
	Err   error        // EventDisconnected only: why the connection ended
}

// Emit hands one event to the pipeline. It blocks while the pipeline is busy
// (backpressure) and returns ctx.Err() if ctx ends first.
type Emit func(ctx context.Context, ev Event) error

// Feed is one exchange (or chain) connection carrying one or more symbols.
type Feed interface {
	Exchange() string
	// Symbols lists the symbols the feed subscribes to, in the exchange's spelling.
	Symbols() []string
	// Kind says what the feed's streams carry: trades (EventTrade) or chain
	// blocks (EventBlock). The pipeline reads it once, when it registers the
	// feed's streams; see docs/adr/0007.
	Kind() domain.StreamKind
	// Run connects, keeps reconnecting on failure and emits events until ctx ends.
	// It returns nil on cancellation and an error only for unrecoverable problems.
	Run(ctx context.Context, emit Emit) error
}

// IncidentStore persists quality incidents.
type IncidentStore interface {
	Save(ctx context.Context, inc domain.Incident) error
}

// BarSink persists completed minute bars.
type BarSink interface {
	WriteBar(ctx context.Context, b domain.Bar) error
}

// Metrics receives the pipeline's measurements. The Prometheus implementation
// lives in internal/platform/metrics; tests use a fake.
type Metrics interface {
	TradeReceived(exchange, symbol string, lag time.Duration)
	IncidentRecorded(exchange, symbol string, kind domain.IncidentKind)
	LateTrade(exchange, symbol string)
	SetConnected(exchange string, up bool)
	Backpressure(exchange string, blocked time.Duration)
	SinkError(sink string)
}

// IncidentFilter narrows an incident query. Zero fields match everything.
type IncidentFilter struct {
	Exchange string
	Symbol   string
	Kind     domain.IncidentKind
	Since    time.Time
	// Limit caps the number of results; the newest incidents come first.
	Limit int
}

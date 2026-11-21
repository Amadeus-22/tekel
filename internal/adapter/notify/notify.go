// Package notify tells someone about incidents the moment they are recorded.
//
// Store wraps the incident store as a decorator: every incident is saved
// first, then the kinds the operator chose are queued and sent by one
// goroutine to each configured Sender (a webhook, Telegram). The pipeline is
// never blocked on the network: when the queue is full the notification is
// dropped (the incident is already saved) and counted, and a flapping stream
// gets at most one message per kind per Interval.
package notify

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
)

// Sender delivers one message.
type Sender interface {
	// Name labels the sender in logs and metrics, e.g. "telegram".
	Name() string
	Send(ctx context.Context, inc domain.Incident, text string) error
}

// Outcome of one notification, for the metric.
const (
	Sent    = "sent"
	Failed  = "failed"
	Dropped = "dropped" // the queue was full
	Limited = "limited" // same stream and kind notified less than Interval ago
)

// Config tunes a Store.
type Config struct {
	// Kinds lists the incident kinds to notify; others are only saved.
	Kinds []domain.IncidentKind
	// Queue bounds the notifications waiting to be sent.
	Queue int
	// Interval is the minimum time between two messages for the same
	// stream and kind.
	Interval time.Duration
	// Timeout bounds each send.
	Timeout time.Duration
	Logger  *slog.Logger
	// Count records an outcome per sender ("" for queue outcomes); nil is allowed.
	Count func(sender, outcome string)
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// DefaultConfig notifies silent gaps and stale streams.
func DefaultConfig() Config {
	return Config{
		Kinds:    []domain.IncidentKind{domain.KindSequenceGap, domain.KindStaleStream},
		Queue:    256,
		Interval: time.Minute,
		Timeout:  10 * time.Second,
	}
}

// Store saves through next and notifies senders.
type Store struct {
	next    app.IncidentStore
	senders []Sender
	cfg     Config
	kinds   map[domain.IncidentKind]bool
	queue   chan domain.Incident

	mu   sync.Mutex
	last map[string]time.Time
}

var _ app.IncidentStore = (*Store)(nil)

// New returns a Store; call Run to start delivering.
func New(next app.IncidentStore, senders []Sender, cfg Config) (*Store, error) {
	switch {
	case next == nil:
		return nil, fmt.Errorf("notify: incident store is required")
	case len(senders) == 0:
		return nil, fmt.Errorf("notify: at least one sender is required")
	case cfg.Logger == nil:
		return nil, fmt.Errorf("notify: logger is required")
	case cfg.Queue <= 0 || cfg.Timeout <= 0 || cfg.Interval < 0:
		return nil, fmt.Errorf("notify: queue and timeout must be positive, interval not negative")
	}
	if cfg.Count == nil {
		cfg.Count = func(string, string) {}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	kinds := make(map[domain.IncidentKind]bool, len(cfg.Kinds))
	for _, k := range cfg.Kinds {
		kinds[k] = true
	}
	return &Store{next: next, senders: senders, cfg: cfg, kinds: kinds,
		queue: make(chan domain.Incident, cfg.Queue), last: map[string]time.Time{}}, nil
}

// Save saves inc through the wrapped store, then queues a notification if
// its kind was chosen. It never waits for the network.
func (s *Store) Save(ctx context.Context, inc domain.Incident) error {
	err := s.next.Save(ctx, inc)
	if !s.kinds[inc.Kind] || !s.allow(inc) {
		return err
	}
	select {
	case s.queue <- inc:
	default:
		s.cfg.Count("", Dropped)
		s.cfg.Logger.Warn("notification dropped: queue full", "exchange", inc.Exchange, "symbol", inc.Symbol, "kind", string(inc.Kind))
	}
	return err
}

// allow applies the per stream and kind interval.
func (s *Store) allow(inc domain.Incident) bool {
	key := inc.Exchange + "\x00" + inc.Symbol + "\x00" + string(inc.Kind)
	now := s.cfg.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.last[key]; ok && now.Sub(t) < s.cfg.Interval {
		s.cfg.Count("", Limited)
		return false
	}
	s.last[key] = now
	return true
}

// Run delivers queued notifications until ctx ends. What is still queued at
// shutdown is not sent: the incidents themselves are saved.
func (s *Store) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case inc := <-s.queue:
			text := Message(inc)
			for _, snd := range s.senders {
				sctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
				err := snd.Send(sctx, inc, text)
				cancel()
				if err != nil {
					s.cfg.Count(snd.Name(), Failed)
					s.cfg.Logger.Warn("notification failed", "sender", snd.Name(), "kind", string(inc.Kind), "error", err)
					continue
				}
				s.cfg.Count(snd.Name(), Sent)
			}
		}
	}
}

// Message is the one-line text every sender posts.
func Message(inc domain.Incident) string {
	var b strings.Builder
	fmt.Fprintf(&b, "tekel: %s %s %s", inc.Exchange, inc.Symbol, inc.Kind)
	if inc.Detail != "" {
		b.WriteString(" — " + inc.Detail)
	}
	if !inc.Start.IsZero() && !inc.End.IsZero() {
		fmt.Fprintf(&b, " (%s → %s UTC)", inc.Start.UTC().Format("2006-01-02 15:04:05"), inc.End.UTC().Format("15:04:05"))
	}
	return b.String()
}

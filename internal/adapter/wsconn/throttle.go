package wsconn

import (
	"sync"
	"time"
)

// Throttle lets one event through per interval and counts the rest. Adapters use
// it to log malformed messages at most once a minute while the metric still
// counts every one.
type Throttle struct {
	mu         sync.Mutex
	every      time.Duration
	last       time.Time
	suppressed int
}

// NewThrottle returns a Throttle that allows one event per every.
func NewThrottle(every time.Duration) *Throttle { return &Throttle{every: every} }

// Allow reports whether an event at now may be logged and, if so, how many
// events were suppressed since the last allowed one.
func (t *Throttle) Allow(now time.Time) (ok bool, suppressed int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.last.IsZero() && now.Sub(t.last) < t.every {
		t.suppressed++
		return false, 0
	}
	t.last = now
	suppressed, t.suppressed = t.suppressed, 0
	return true, suppressed
}

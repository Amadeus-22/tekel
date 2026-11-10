package wsconn

import (
	"log/slog"
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

// Malformed reports messages a feed could not decode: every one is counted,
// and one sample is logged per minute so a broken upstream cannot flood the
// logs. Each adapter owns one.
type Malformed struct {
	exchange string
	count    func()
	log      *slog.Logger
	throttle *Throttle
}

// NewMalformed returns a reporter for exchange. count is called once per
// malformed message (the decode-error metric); nil is allowed.
func NewMalformed(exchange string, log *slog.Logger, count func()) *Malformed {
	if count == nil {
		count = func() {}
	}
	return &Malformed{exchange: exchange, count: count, log: log, throttle: NewThrottle(time.Minute)}
}

// Report counts msg and logs it with err unless a sample was logged recently.
func (m *Malformed) Report(msg []byte, now time.Time, err error) {
	m.count()
	if ok, suppressed := m.throttle.Allow(now); ok {
		sample := string(msg)
		if len(sample) > 200 {
			sample = sample[:200] + "…"
		}
		m.log.Warn("malformed message skipped", "exchange", m.exchange,
			"error", err.Error(), "sample", sample, "suppressed_since_last_log", suppressed)
	}
}

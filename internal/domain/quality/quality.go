// Package quality holds Tekel's data-quality validators.
//
// A Stream carries the state of one exchange/symbol feed and turns each observed
// event (a trade, a disconnect, a reconnect, the passage of time) into zero or more
// incidents. It is pure: time is always passed in, nothing is read from a clock or
// written anywhere, so every rule is testable with plain values.
package quality

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

// madToSigma rescales the median absolute deviation so that, for normally
// distributed prices, k × MAD is comparable to k standard deviations.
const madToSigma = 1.4826

// Config holds the thresholds of every check. Zero values are not meaningful;
// start from DefaultConfig.
type Config struct {
	// ClockSkew flags a trade whose |ReceivedTime − ExchangeTime| exceeds it.
	ClockSkew time.Duration
	// OutOfOrderTolerance is how far back in exchange time a trade may go before
	// it is flagged as out of order.
	OutOfOrderTolerance time.Duration
	// StaleAfter flags a connected stream that saw no trade (or block) for this long.
	StaleAfter time.Duration
	// DuplicateWindow is how many recent trade IDs are remembered.
	DuplicateWindow int
	// SpikeWindow is how many recent prices feed the rolling median.
	SpikeWindow int
	// SpikeMinSamples is how many prices must be in the window before spikes are checked.
	SpikeMinSamples int
	// SpikeK is the number of (scaled) MADs a price may deviate from the median.
	SpikeK float64
	// SpikeMinDeviation is a relative floor (0.01 = 1%) on the allowed deviation, so
	// that a window of identical prices (MAD = 0) does not flag every tick.
	SpikeMinDeviation float64
}

// DefaultConfig returns thresholds that suit liquid spot pairs.
func DefaultConfig() Config {
	return Config{
		ClockSkew:           2 * time.Second,
		OutOfOrderTolerance: 0,
		StaleAfter:          60 * time.Second,
		DuplicateWindow:     10_000,
		SpikeWindow:         101,
		SpikeMinSamples:     21,
		SpikeK:              10,
		SpikeMinDeviation:   0.005,
	}
}

// Validate reports the first nonsensical threshold.
func (c Config) Validate() error {
	var errs []error
	if c.ClockSkew <= 0 {
		errs = append(errs, errors.New("clock skew threshold must be positive"))
	}
	if c.OutOfOrderTolerance < 0 {
		errs = append(errs, errors.New("out-of-order tolerance must not be negative"))
	}
	if c.StaleAfter <= 0 {
		errs = append(errs, errors.New("stale threshold must be positive"))
	}
	if c.DuplicateWindow < 1 {
		errs = append(errs, errors.New("duplicate window must be at least 1"))
	}
	if c.SpikeWindow < 3 {
		errs = append(errs, errors.New("spike window must be at least 3"))
	}
	if c.SpikeMinSamples < 3 || c.SpikeMinSamples > c.SpikeWindow {
		errs = append(errs, errors.New("spike min samples must be between 3 and the spike window"))
	}
	if c.SpikeK <= 0 {
		errs = append(errs, errors.New("spike k must be positive"))
	}
	if c.SpikeMinDeviation < 0 {
		errs = append(errs, errors.New("spike min deviation must not be negative"))
	}
	return errors.Join(errs...)
}

// Verdict is the outcome of observing one trade.
type Verdict struct {
	Incidents []domain.Incident
	// Drop is true when the trade must not reach the sinks. Only duplicates are
	// dropped: they are provably not new information. Every other incident flags
	// a trade that really happened (a spike can be a genuine fat-finger trade).
	Drop bool
}

// Stream is the validation state of one exchange/symbol feed. It is not safe for
// concurrent use; the pipeline owns each Stream from a single goroutine.
type Stream struct {
	key   domain.StreamKey
	cfg   Config
	words words

	connected    bool
	downSince    time.Time
	lastActivity time.Time // last trade, or (re)connect time; drives staleness
	staleSince   time.Time // non-zero while a stale incident is open

	haveLast     bool // false until the first trade after (re)connect
	lastID       int64
	lastExchTime time.Time

	seen   map[int64]struct{}
	seenQ  []int64 // ring buffer of the IDs in seen, oldest at seenAt
	seenAt int

	spikes *SpikeWindow
}

// words name what a stream's sequence numbers count, for incident details.
type words struct{ id, unit, units, time string }

var (
	tradeWords = words{id: "trade_id", unit: "trade", units: "trades", time: "exchange time"}
	blockWords = words{id: "nonce", unit: "block", units: "blocks", time: "block time"}
)

// NewStream creates the state for one trade stream. cfg must be valid; per-symbol
// thresholds (such as StaleAfter for an illiquid pair) are set in cfg by the
// caller. The stream starts disconnected; call Connected when the feed is up.
func NewStream(key domain.StreamKey, cfg Config) *Stream {
	return &Stream{
		key:    key,
		cfg:    cfg,
		words:  tradeWords,
		seen:   make(map[int64]struct{}, cfg.DuplicateWindow),
		seenQ:  make([]int64, 0, cfg.DuplicateWindow),
		spikes: NewSpikeWindow(cfg.SpikeRule()),
	}
}

// NewBlockStream creates the state for one chain block stream: the same rules
// over the block nonce, observed with ObserveBlock. The spike settings in cfg
// are unused.
func NewBlockStream(key domain.StreamKey, cfg Config) *Stream {
	s := NewStream(key, cfg)
	s.words = blockWords
	return s
}

// Key returns the stream's identity.
func (s *Stream) Key() domain.StreamKey { return s.key }

// IsConnected reports whether the stream's connection is currently up.
func (s *Stream) IsConnected() bool { return s.connected }

// Observe runs every per-trade check against t and updates the stream state.
func (s *Stream) Observe(t domain.Trade) Verdict {
	v := s.observe(t.TradeID, t.ExchangeTime, t.ReceivedTime)
	if v.Drop {
		return v
	}
	if inc, ok := s.checkSpike(t); ok {
		v.Incidents = append(v.Incidents, inc)
	}
	return v
}

// ObserveBlock runs the checks that apply to a chain block (duplicate,
// sequence_gap over the nonce, out_of_order, clock_skew) and updates the stream
// state. A block has no price, so there is no spike check.
func (s *Stream) ObserveBlock(b domain.Block) Verdict {
	return s.observe(b.Nonce, b.ChainTime, b.ReceivedTime)
}

// observe runs the checks shared by every stream kind on one event with
// sequence number id.
func (s *Stream) observe(id int64, exchTime, received time.Time) Verdict {
	s.lastActivity = received
	s.staleSince = time.Time{}
	w := s.words

	if _, dup := s.seen[id]; dup {
		return Verdict{Drop: true, Incidents: []domain.Incident{s.incident(domain.KindDuplicate, id, received,
			fmt.Sprintf("%s %d already seen in the last %d %s", w.id, id, s.cfg.DuplicateWindow, w.units))}}
	}
	s.remember(id)

	var out []domain.Incident
	if s.haveLast {
		if id > s.lastID+1 {
			inc := s.incident(domain.KindSequenceGap, id, received, fmt.Sprintf(
				"%s jumped from %d to %d while connected (%d missing)",
				w.id, s.lastID, id, id-s.lastID-1))
			inc.Start, inc.End = s.lastExchTime, exchTime
			out = append(out, inc)
		}
		if behind := s.lastExchTime.Sub(exchTime); behind > s.cfg.OutOfOrderTolerance {
			out = append(out, s.incident(domain.KindOutOfOrder, id, received, fmt.Sprintf(
				"%s %s is %s earlier than the previous %s",
				w.time, exchTime.Format(time.RFC3339Nano), behind, w.unit)))
		}
	}
	if skew := received.Sub(exchTime); skew.Abs() > s.cfg.ClockSkew {
		out = append(out, s.incident(domain.KindClockSkew, id, received, fmt.Sprintf(
			"received - %s = %s exceeds %s", w.time, skew, s.cfg.ClockSkew)))
	}

	if !s.haveLast || id > s.lastID {
		s.lastID = id
	}
	if !s.haveLast || exchTime.After(s.lastExchTime) {
		s.lastExchTime = exchTime
	}
	s.haveLast = true
	return Verdict{Incidents: out}
}

// Connected marks the connection as up at time at. If the stream was down, it
// returns the disconnect incident describing the known gap [downSince, at].
//
// The sequence baseline is reset: trades missed while disconnected are a known
// gap, so the first trade after a reconnect must not raise a sequence_gap.
func (s *Stream) Connected(at time.Time) (domain.Incident, bool) {
	wasDown := !s.connected && !s.downSince.IsZero()
	s.connected = true
	s.haveLast = false
	s.lastActivity = at
	s.staleSince = time.Time{}
	if !wasDown {
		return domain.Incident{}, false
	}
	inc := domain.Incident{
		Exchange: s.key.Exchange, Symbol: s.key.Symbol, Kind: domain.KindDisconnect,
		DetectedAt: at, Start: s.downSince, End: at,
		Detail: fmt.Sprintf("connection down for %s; last %s before the gap: %d",
			at.Sub(s.downSince).Round(time.Millisecond), s.words.id, s.lastID),
	}
	s.downSince = time.Time{}
	return inc, true
}

// Disconnected marks the connection as lost at time at. The incident is emitted
// by the next Connected call, once the window is known; see OpenGap for shutdown.
func (s *Stream) Disconnected(at time.Time) {
	if s.connected || s.downSince.IsZero() {
		s.downSince = at
	}
	s.connected = false
}

// OpenGap returns the disconnect incident for a gap that is still open at time
// now, for example when Tekel shuts down while an exchange is unreachable.
func (s *Stream) OpenGap(now time.Time) (domain.Incident, bool) {
	if s.connected || s.downSince.IsZero() {
		return domain.Incident{}, false
	}
	return domain.Incident{
		Exchange: s.key.Exchange, Symbol: s.key.Symbol, Kind: domain.KindDisconnect,
		DetectedAt: now, Start: s.downSince, End: now,
		Detail: fmt.Sprintf("connection still down after %s; window closed at shutdown",
			now.Sub(s.downSince).Round(time.Millisecond)),
	}, true
}

// CheckStale reports a stale_stream incident when a connected stream has seen no
// trade for longer than staleAfter. It fires once per stale episode; the next
// trade or reconnect re-arms it. A disconnected stream is never stale: that
// silence is already explained by the disconnect.
func (s *Stream) CheckStale(now time.Time) (domain.Incident, bool) {
	staleAfter := s.cfg.StaleAfter
	if !s.connected || !s.staleSince.IsZero() || s.lastActivity.IsZero() {
		return domain.Incident{}, false
	}
	idle := now.Sub(s.lastActivity)
	if idle <= staleAfter {
		return domain.Incident{}, false
	}
	s.staleSince = s.lastActivity
	return domain.Incident{
		Exchange: s.key.Exchange, Symbol: s.key.Symbol, Kind: domain.KindStaleStream,
		DetectedAt: now, Start: s.lastActivity, End: now,
		Detail: fmt.Sprintf("no %s for %s (threshold %s)", s.words.unit, idle.Round(time.Millisecond), staleAfter),
	}, true
}

// incident builds a point incident about the event with sequence number id.
// For a block stream the incident's TradeID holds the block nonce.
func (s *Stream) incident(kind domain.IncidentKind, id int64, received time.Time, detail string) domain.Incident {
	return domain.Incident{
		Exchange: s.key.Exchange, Symbol: s.key.Symbol, Kind: kind,
		DetectedAt: received, TradeID: id, Detail: detail,
	}
}

// remember adds id to the duplicate window, evicting the oldest ID when full.
func (s *Stream) remember(id int64) {
	if len(s.seenQ) < s.cfg.DuplicateWindow {
		s.seenQ = append(s.seenQ, id)
	} else {
		delete(s.seen, s.seenQ[s.seenAt])
		s.seenQ[s.seenAt] = id
		s.seenAt = (s.seenAt + 1) % s.cfg.DuplicateWindow
	}
	s.seen[id] = struct{}{}
}

// checkSpike compares t's price with the rolling median of previous prices, then
// adds the price to the window.
func (s *Stream) checkSpike(t domain.Trade) (domain.Incident, bool) {
	price := t.Price.Float64()
	defer s.spikes.Add(price)
	r, spike := s.spikes.Check(price)
	if !spike {
		return domain.Incident{}, false
	}
	return s.incident(domain.KindPriceSpike, t.TradeID, t.ReceivedTime, fmt.Sprintf(
		"price %s deviates %.6g from rolling median %.6g (allowed %.6g, MAD %.6g, n=%d)",
		t.Price, r.Deviation, r.Median, r.Allowed, r.MAD, r.N)), true
}

// medianMAD returns the median of xs and the median absolute deviation from it.
// scratch is reused to avoid allocating on every trade.
func medianMAD(xs, scratch []float64) (median, mad float64) {
	scratch = append(scratch[:0], xs...)
	median = medianInPlace(scratch)
	for i, x := range xs {
		scratch[i] = math.Abs(x - median)
	}
	return median, medianInPlace(scratch)
}

func medianInPlace(xs []float64) float64 {
	slices.Sort(xs)
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

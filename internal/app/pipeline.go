package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/bars"
	"github.com/Amadeus-22/tekel/internal/domain/quality"
)

// Sink names used in the SinkError metric.
const (
	SinkIncidents = "incidents"
	SinkBars      = "bars"
)

// Config tunes the pipeline.
type Config struct {
	Quality quality.Config
	// StaleAfter overrides Quality.StaleAfter for individual streams, because an
	// illiquid pair can be silent for minutes without anything being wrong.
	StaleAfter map[domain.StreamKey]time.Duration
	// Buffer is the capacity of the channel between feeds and the validator.
	// When it is full, feeds block: see docs/adr/0003.
	Buffer int
	// TickInterval is how often staleness is checked and finished bars flushed.
	TickInterval time.Duration
	// BarGrace is how long after a minute ends its bar waits for late trades.
	BarGrace time.Duration
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		Quality:      quality.DefaultConfig(),
		Buffer:       4096,
		TickInterval: time.Second,
		BarGrace:     2 * time.Second,
	}
}

// Deps are the pipeline's collaborators.
type Deps struct {
	Feeds     []Feed
	Incidents IncidentStore
	Bars      BarSink
	Metrics   Metrics
	Logger    *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// StreamStatus is a point-in-time view of one stream, served by GET /v1/streams.
type StreamStatus struct {
	Exchange  string
	Symbol    string
	Kind      domain.StreamKind
	Connected bool
	// Trades counts accepted events: trades, or blocks on a block stream.
	Trades uint64
	// LastTradeID is the last trade ID, or the last block nonce.
	LastTradeID      int64
	LastPrice        string
	LastExchangeTime time.Time
	LastReceivedTime time.Time
	// Lag is ReceivedTime − ExchangeTime of the last trade.
	Lag time.Duration
	// Incidents counts incidents by kind since the process started.
	Incidents map[domain.IncidentKind]int
}

// stream is the pipeline's per-stream state. quality and agg are touched only by
// the consumer goroutine; status is shared with API readers under Pipeline.mu.
// A block stream has an aggregator too, but blocks never reach it, so it never
// produces a bar.
type stream struct {
	quality *quality.Stream
	agg     *bars.Aggregator
	status  StreamStatus
}

// Pipeline fans in all feeds, validates each trade, records incidents and turns
// trades into minute bars. Create it with NewPipeline and start it with Run.
type Pipeline struct {
	cfg       Config
	feeds     []Feed
	incidents IncidentStore
	bars      BarSink
	metrics   Metrics
	log       *slog.Logger
	now       func() time.Time

	mu      sync.RWMutex
	streams map[domain.StreamKey]*stream
	order   []domain.StreamKey // stable iteration order
}

// NewPipeline validates cfg and deps and pre-registers every configured stream,
// so that a symbol which never trades still shows up (and can go stale).
func NewPipeline(cfg Config, d Deps) (*Pipeline, error) {
	var errs []error
	if err := cfg.Quality.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("quality config: %w", err))
	}
	if cfg.Buffer < 1 {
		errs = append(errs, errors.New("buffer must be at least 1"))
	}
	if cfg.TickInterval <= 0 {
		errs = append(errs, errors.New("tick interval must be positive"))
	}
	if cfg.BarGrace < 0 {
		errs = append(errs, errors.New("bar grace must not be negative"))
	}
	if len(d.Feeds) == 0 {
		errs = append(errs, errors.New("at least one feed is required"))
	}
	if d.Incidents == nil || d.Bars == nil || d.Metrics == nil || d.Logger == nil {
		errs = append(errs, errors.New("incident store, bar sink, metrics and logger are required"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("new pipeline: %w", err)
	}
	now := d.Now
	if now == nil {
		now = time.Now
	}
	p := &Pipeline{
		cfg: cfg, feeds: d.Feeds, incidents: d.Incidents, bars: d.Bars,
		metrics: d.Metrics, log: d.Logger, now: now,
		streams: make(map[domain.StreamKey]*stream),
	}
	for _, f := range d.Feeds {
		for _, sym := range f.Symbols() {
			p.stream(domain.StreamKey{Exchange: f.Exchange(), Symbol: sym}, f.Kind())
		}
	}
	// An override for a stream no feed carries is a typo or a disabled feed;
	// ignoring it would leave the stream it was meant for on the default.
	var unknown []string
	for key := range cfg.StaleAfter {
		if _, ok := p.streams[key]; !ok {
			unknown = append(unknown, key.String())
		}
	}
	if len(unknown) > 0 {
		slices.Sort(unknown)
		return nil, fmt.Errorf("new pipeline: stale override for %s matches no configured stream", strings.Join(unknown, ", "))
	}
	return p, nil
}

// Run starts every feed and the validator, and blocks until ctx is cancelled or
// a feed fails for good. On the way out it drains buffered events, flushes open
// bars and closes open disconnect windows, so a clean shutdown loses nothing.
func (p *Pipeline) Run(ctx context.Context) error {
	events := make(chan Event, p.cfg.Buffer)

	// Sinks must keep working while we drain after ctx is cancelled.
	sinkCtx := context.WithoutCancel(ctx)
	consumed := make(chan struct{})
	go func() {
		defer close(consumed)
		p.consume(sinkCtx, events)
	}()

	g, gctx := errgroup.WithContext(ctx)
	for _, f := range p.feeds {
		g.Go(func() error {
			if err := f.Run(gctx, p.emitter(events, f.Exchange())); err != nil {
				return fmt.Errorf("feed %s: %w", f.Exchange(), err)
			}
			return nil
		})
	}
	err := g.Wait()
	close(events)
	<-consumed
	return err
}

// emitter returns the Emit function for one feed. It stamps every event with
// the feed's exchange, so feeds do not have to. A send that finds the buffer
// full blocks, and the time spent blocked is reported as backpressure: Tekel
// never drops a trade to keep up (docs/adr/0003).
func (p *Pipeline) emitter(events chan<- Event, exchange string) Emit {
	return func(ctx context.Context, ev Event) error {
		ev.Exchange = exchange
		select {
		case events <- ev:
			return nil
		default:
		}
		start := time.Now()
		defer func() { p.metrics.Backpressure(exchange, time.Since(start)) }()
		select {
		case events <- ev:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *Pipeline) consume(ctx context.Context, events <-chan Event) {
	ticker := time.NewTicker(p.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				p.finish(ctx)
				return
			}
			p.handle(ctx, ev)
		case <-ticker.C:
			p.tick(ctx, p.now())
		}
	}
}

func (p *Pipeline) handle(ctx context.Context, ev Event) {
	switch ev.Kind {
	case EventTrade:
		p.handleTrade(ctx, ev.Trade)
	case EventBlock:
		p.handleBlock(ctx, ev.Block)
	case EventConnected:
		p.metrics.SetConnected(ev.Exchange, true)
		p.log.Info("feed connected", "exchange", ev.Exchange)
		for _, s := range p.exchangeStreams(ev.Exchange) {
			if inc, ok := s.quality.Connected(ev.At); ok {
				p.record(ctx, inc)
			}
			p.setConnected(s, true)
		}
	case EventDisconnected:
		p.metrics.SetConnected(ev.Exchange, false)
		p.log.Warn("feed disconnected", "exchange", ev.Exchange, "error", errString(ev.Err))
		for _, s := range p.exchangeStreams(ev.Exchange) {
			s.quality.Disconnected(ev.At)
			p.setConnected(s, false)
		}
	default:
		p.log.Error("unknown event kind", "kind", int(ev.Kind), "exchange", ev.Exchange)
	}
}

func (p *Pipeline) handleTrade(ctx context.Context, t domain.Trade) {
	s := p.stream(t.Key(), domain.StreamTrades)
	if !p.accept(ctx, s, s.quality.Observe(t), t.TradeID, t.Price.String(), t.ExchangeTime, t.ReceivedTime) {
		return
	}

	done, ok, err := s.agg.Add(t)
	switch {
	case errors.Is(err, domain.ErrLateTrade):
		p.metrics.LateTrade(t.Exchange, t.Symbol)
		p.log.Debug("late trade not in bars", "exchange", t.Exchange, "symbol", t.Symbol, "error", err)
	case err != nil:
		p.log.Error("aggregate trade", "exchange", t.Exchange, "symbol", t.Symbol, "error", err)
	case ok:
		p.writeBar(ctx, done)
	}
}

// handleBlock validates a chain block. Blocks have no price and are not bars:
// only the stream status, incidents and metrics see them.
func (p *Pipeline) handleBlock(ctx context.Context, b domain.Block) {
	s := p.stream(b.Key(), domain.StreamBlocks)
	p.accept(ctx, s, s.quality.ObserveBlock(b), b.Nonce, "", b.ChainTime, b.ReceivedTime)
}

// accept records the incidents of verdict v for the event with sequence number
// id and, unless v drops the event, updates the stream status. It reports
// whether the event was kept.
func (p *Pipeline) accept(ctx context.Context, s *stream, v quality.Verdict, id int64, price string,
	exchTime, received time.Time) bool {
	lag := received.Sub(exchTime)
	p.metrics.TradeReceived(s.status.Exchange, s.status.Symbol, lag)
	for _, inc := range v.Incidents {
		p.record(ctx, inc)
	}
	if v.Drop {
		return false
	}
	p.mu.Lock()
	st := &s.status
	st.Trades++
	st.LastTradeID = id
	st.LastPrice = price
	st.LastExchangeTime = exchTime
	st.LastReceivedTime = received
	st.Lag = lag
	p.mu.Unlock()
	return true
}

// tick runs the time-driven work: staleness checks and flushing finished bars.
func (p *Pipeline) tick(ctx context.Context, now time.Time) {
	cutoff := now.Add(-p.cfg.BarGrace)
	for _, s := range p.allStreams() {
		if inc, ok := s.quality.CheckStale(now); ok {
			p.record(ctx, inc)
		}
		if b, ok := s.agg.FlushBefore(cutoff); ok {
			p.writeBar(ctx, b)
		}
	}
}

// finish runs once after the last event: partial bars are written (a restart
// merges them, see leanfs) and still-open disconnect windows are recorded.
func (p *Pipeline) finish(ctx context.Context) {
	now := p.now()
	for _, s := range p.allStreams() {
		if b, ok := s.agg.Flush(); ok {
			p.writeBar(ctx, b)
		}
		if inc, ok := s.quality.OpenGap(now); ok {
			p.record(ctx, inc)
		}
	}
}

func (p *Pipeline) record(ctx context.Context, inc domain.Incident) {
	p.metrics.IncidentRecorded(inc.Exchange, inc.Symbol, inc.Kind)
	p.mu.Lock()
	if s, ok := p.streams[inc.Key()]; ok {
		s.status.Incidents[inc.Kind]++
	}
	p.mu.Unlock()
	p.log.Info("quality incident", "exchange", inc.Exchange, "symbol", inc.Symbol,
		"kind", string(inc.Kind), "trade_id", inc.TradeID, "detail", inc.Detail)
	if err := p.incidents.Save(ctx, inc); err != nil {
		p.metrics.SinkError(SinkIncidents)
		p.log.Error("save incident", "kind", string(inc.Kind), "error", err)
	}
}

func (p *Pipeline) writeBar(ctx context.Context, b domain.Bar) {
	if err := p.bars.WriteBar(ctx, b); err != nil {
		p.metrics.SinkError(SinkBars)
		p.log.Error("write bar", "exchange", b.Exchange, "symbol", b.Symbol,
			"minute", b.Start.Format(time.RFC3339), "error", err)
	}
}

// stream returns the state for key, creating it on first use. kind is the
// stream's kind; it is the one place where trade and block streams differ:
// a block stream's validator has no price check (and its aggregator is never
// fed, see handleBlock).
func (p *Pipeline) stream(key domain.StreamKey, kind domain.StreamKind) *stream {
	p.mu.RLock()
	s, ok := p.streams[key]
	p.mu.RUnlock()
	if ok {
		return s
	}
	qcfg := p.cfg.Quality
	if d, ok := p.cfg.StaleAfter[key]; ok {
		qcfg.StaleAfter = d
	}
	newValidator := quality.NewStream
	if kind == domain.StreamBlocks {
		newValidator = quality.NewBlockStream
	}
	s = &stream{
		quality: newValidator(key, qcfg),
		agg:     bars.NewAggregator(key),
		status: StreamStatus{
			Exchange: key.Exchange, Symbol: key.Symbol, Kind: kind,
			Incidents: make(map[domain.IncidentKind]int),
		},
	}
	// A stream first seen through a trade inherits the feed's connection state.
	for _, other := range p.exchangeStreams(key.Exchange) {
		if other.quality.IsConnected() {
			s.quality.Connected(p.now())
			s.status.Connected = true
			break
		}
	}
	p.mu.Lock()
	p.streams[key] = s
	p.order = append(p.order, key)
	p.mu.Unlock()
	return s
}

func (p *Pipeline) setConnected(s *stream, up bool) {
	p.mu.Lock()
	s.status.Connected = up
	p.mu.Unlock()
}

func (p *Pipeline) allStreams() []*stream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*stream, 0, len(p.order))
	for _, k := range p.order {
		out = append(out, p.streams[k])
	}
	return out
}

func (p *Pipeline) exchangeStreams(exchange string) []*stream {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []*stream
	for _, k := range p.order {
		if k.Exchange == exchange {
			out = append(out, p.streams[k])
		}
	}
	return out
}

// Streams returns a snapshot of every stream, sorted by exchange then symbol.
// It is safe to call from any goroutine.
func (p *Pipeline) Streams() []StreamStatus {
	p.mu.RLock()
	out := make([]StreamStatus, 0, len(p.streams))
	for _, s := range p.streams {
		st := s.status
		st.Incidents = maps.Clone(s.status.Incidents)
		out = append(out, st)
	}
	p.mu.RUnlock()
	slices.SortFunc(out, func(a, b StreamStatus) int {
		return cmp.Or(cmp.Compare(a.Exchange, b.Exchange), cmp.Compare(a.Symbol, b.Symbol))
	})
	return out
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

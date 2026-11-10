package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// --- fakes ------------------------------------------------------------------

type fakeFeed struct {
	exchange string
	symbols  []string
	kind     domain.StreamKind // zero means trades
	events   []Event
	err      error         // returned by Run after emitting, instead of waiting
	emitted  chan struct{} // closed once every event was handed over
}

func newFakeFeed(exchange string, symbols []string, events ...Event) *fakeFeed {
	return &fakeFeed{exchange: exchange, symbols: symbols, events: events, emitted: make(chan struct{})}
}

func (f *fakeFeed) Exchange() string  { return f.exchange }
func (f *fakeFeed) Symbols() []string { return f.symbols }
func (f *fakeFeed) Kind() domain.StreamKind {
	if f.kind == "" {
		return domain.StreamTrades
	}
	return f.kind
}
func (f *fakeFeed) Run(ctx context.Context, emit Emit) error {
	for _, ev := range f.events {
		if err := emit(ctx, ev); err != nil {
			return nil // cancelled
		}
	}
	close(f.emitted)
	if f.err != nil {
		return f.err
	}
	<-ctx.Done()
	return nil
}

type fakeStore struct {
	mu      sync.Mutex
	saved   []domain.Incident
	err     error
	release chan struct{} // if set, Save blocks until closed
}

func (s *fakeStore) Save(_ context.Context, inc domain.Incident) error {
	if s.release != nil {
		<-s.release
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, inc)
	return s.err
}

func (s *fakeStore) kinds() []domain.IncidentKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.IncidentKind
	for _, i := range s.saved {
		out = append(out, i.Kind)
	}
	return out
}

type fakeSink struct {
	mu   sync.Mutex
	bars []domain.Bar
	err  error
}

func (s *fakeSink) WriteBar(_ context.Context, b domain.Bar) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bars = append(s.bars, b)
	return s.err
}

func (s *fakeSink) written() []domain.Bar {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Bar(nil), s.bars...)
}

type fakeMetrics struct {
	mu           sync.Mutex
	trades       int
	incidents    map[domain.IncidentKind]int
	late         int
	connected    map[string]bool
	backpressure time.Duration
	sinkErrors   map[string]int
}

func newFakeMetrics() *fakeMetrics {
	return &fakeMetrics{
		incidents: map[domain.IncidentKind]int{}, connected: map[string]bool{}, sinkErrors: map[string]int{},
	}
}

func (m *fakeMetrics) TradeReceived(string, string, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trades++
}

func (m *fakeMetrics) IncidentRecorded(_, _ string, k domain.IncidentKind) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.incidents[k]++
}

func (m *fakeMetrics) LateTrade(string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.late++
}

func (m *fakeMetrics) SetConnected(ex string, up bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.connected[ex] = up
}

func (m *fakeMetrics) Backpressure(_ string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backpressure += d
}

func (m *fakeMetrics) SinkError(sink string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinkErrors[sink]++
}

// --- helpers ----------------------------------------------------------------

const ex, sym = domain.ExchangeBinance, "BTCUSDT"

func tradeEv(id int64, at time.Duration, price string) Event {
	ts := t0.Add(at)
	return Event{Kind: EventTrade, Exchange: ex, At: ts, Trade: domain.Trade{
		Exchange: ex, Symbol: sym, TradeID: id, Price: domain.MustDecimal(price),
		Qty: domain.MustDecimal("1"), Side: domain.SideBuy, ExchangeTime: ts, ReceivedTime: ts,
	}}
}

func connEv(kind EventKind, at time.Duration) Event {
	return Event{Kind: kind, Exchange: ex, At: t0.Add(at), Err: errors.New("test")}
}

type harness struct {
	p       *Pipeline
	store   *fakeStore
	sink    *fakeSink
	metrics *fakeMetrics
}

func newHarness(t *testing.T, cfg Config, feeds ...Feed) *harness {
	t.Helper()
	h := &harness{store: &fakeStore{}, sink: &fakeSink{}, metrics: newFakeMetrics()}
	p, err := NewPipeline(cfg, Deps{
		Feeds: feeds, Incidents: h.store, Bars: h.sink, Metrics: h.metrics,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:    func() time.Time { return t0.Add(time.Hour) },
	})
	if err != nil {
		t.Fatal(err)
	}
	h.p = p
	return h
}

// runUntilEmitted runs the pipeline until the feed emitted everything, then
// cancels and waits for the drain.
func runUntilEmitted(t *testing.T, h *harness, f *fakeFeed) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.p.Run(ctx) }()
	select {
	case <-f.emitted:
	case <-time.After(5 * time.Second):
		t.Fatal("feed did not finish emitting")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func testConfig() Config {
	cfg := DefaultConfig()
	cfg.TickInterval = time.Hour // time-driven work is tested separately
	return cfg
}

// --- tests ------------------------------------------------------------------

func TestPipelineEndToEnd(t *testing.T) {
	f := newFakeFeed(ex, []string{sym},
		connEv(EventConnected, 0),
		tradeEv(1, 1*time.Second, "100"),
		tradeEv(2, 2*time.Second, "101"),
		tradeEv(5, 3*time.Second, "99"), // silent gap
		tradeEv(5, 4*time.Second, "99"), // duplicate: dropped
		tradeEv(6, 61*time.Second, "100"),
		connEv(EventDisconnected, 62*time.Second),
		connEv(EventConnected, 70*time.Second), // known gap
		tradeEv(40, 71*time.Second, "100.5"),   // no sequence_gap after reconnect
	)
	h := newHarness(t, testConfig(), f)
	runUntilEmitted(t, h, f)

	got := h.store.kinds()
	want := []domain.IncidentKind{domain.KindSequenceGap, domain.KindDuplicate, domain.KindDisconnect}
	if len(got) != len(want) {
		t.Fatalf("incidents = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("incidents = %v, want %v", got, want)
		}
	}

	bars := h.sink.written()
	if len(bars) != 2 {
		t.Fatalf("bars = %d, want 2 (minute 0 on rollover, minute 1 flushed at shutdown)", len(bars))
	}
	first := bars[0]
	if domain.FormatRat(first.Open) != "100" || domain.FormatRat(first.High) != "101" ||
		domain.FormatRat(first.Low) != "99" || domain.FormatRat(first.Volume) != "3" || first.Trades != 3 {
		t.Errorf("first bar = %+v (duplicate must not count)", first)
	}
	if bars[1].Trades != 2 {
		t.Errorf("second bar trades = %d, want 2", bars[1].Trades)
	}

	st := h.p.Streams()
	if len(st) != 1 {
		t.Fatalf("streams = %+v", st)
	}
	s := st[0]
	if !s.Connected || s.Trades != 5 || s.LastTradeID != 40 || s.LastPrice != "100.5" {
		t.Errorf("status = %+v", s)
	}
	if s.Incidents[domain.KindSequenceGap] != 1 || s.Incidents[domain.KindDuplicate] != 1 || s.Incidents[domain.KindDisconnect] != 1 {
		t.Errorf("status incident counts = %v", s.Incidents)
	}
	if h.metrics.trades != 6 || !h.metrics.connected[ex] || h.metrics.incidents[domain.KindDisconnect] != 1 {
		t.Errorf("metrics = %+v", h.metrics)
	}
}

// TestPipelineBlockStream: a block feed gets the sequence, time and connection
// checks, never a bar, and its status says kind=blocks, while a trade feed in the
// same pipeline is unaffected.
func TestPipelineBlockStream(t *testing.T) {
	const chain, network = domain.ChainKlever, "testnet"
	blockEv := func(nonce int64, at time.Duration) Event {
		ts := t0.Add(at)
		return Event{Kind: EventBlock, Exchange: chain, At: ts, Block: domain.Block{
			Chain: chain, Network: network, Nonce: nonce, Hash: fmt.Sprintf("h%d", nonce),
			ChainTime: ts, ReceivedTime: ts.Add(500 * time.Millisecond),
		}}
	}
	up := Event{Kind: EventConnected, Exchange: chain, At: t0}
	blocks := newFakeFeed(chain, []string{network}, up,
		blockEv(100, 4*time.Second),
		blockEv(101, 8*time.Second),
		blockEv(104, 20*time.Second), // silent gap: 102 and 103 never arrived
		blockEv(104, 21*time.Second), // duplicate: dropped
		Event{Kind: EventDisconnected, Exchange: chain, At: t0.Add(22 * time.Second)},
		Event{Kind: EventConnected, Exchange: chain, At: t0.Add(40 * time.Second)}, // known gap
		blockEv(109, 40*time.Second), // no sequence_gap after reconnect
	)
	blocks.kind = domain.StreamBlocks
	trades := newFakeFeed(ex, []string{sym})
	h := newHarness(t, testConfig(), blocks, trades)
	if st := h.p.Streams(); st[0].Kind != domain.StreamTrades || st[1].Kind != domain.StreamBlocks {
		t.Fatalf("kinds before any event = %s, %s", st[0].Kind, st[1].Kind)
	}
	runUntilEmitted(t, h, blocks)

	got := fmt.Sprint(h.store.kinds())
	if want := "[sequence_gap duplicate disconnect]"; got != want {
		t.Fatalf("incidents = %s, want %s", got, want)
	}
	if bars := h.sink.written(); len(bars) != 0 {
		t.Fatalf("a block stream wrote %d bars", len(bars))
	}
	s := h.p.Streams()[1]
	if s.Exchange != chain || s.Symbol != network || s.Kind != domain.StreamBlocks || s.Trades != 4 ||
		s.LastTradeID != 109 || s.LastPrice != "" || s.Lag != 500*time.Millisecond || !s.Connected {
		t.Errorf("status = %+v", s)
	}
}

func TestStreamsSnapshotIsACopy(t *testing.T) {
	f := newFakeFeed(ex, []string{"ETHUSDT", sym}, connEv(EventConnected, 0))
	f2 := newFakeFeed(domain.ExchangeKraken, []string{"BTC/USD"})
	h := newHarness(t, testConfig(), f, f2)
	st := h.p.Streams()
	if len(st) != 3 || st[0].Symbol != "BTCUSDT" || st[1].Symbol != "ETHUSDT" || st[2].Exchange != domain.ExchangeKraken {
		t.Fatalf("Streams() order = %+v", st)
	}
	st[0].Incidents[domain.KindDuplicate] = 99
	if h.p.Streams()[0].Incidents[domain.KindDuplicate] != 0 {
		t.Error("Streams() leaked its internal map")
	}
}

func TestPipelineShutdownClosesOpenGap(t *testing.T) {
	f := newFakeFeed(ex, []string{sym},
		connEv(EventConnected, 0),
		connEv(EventDisconnected, time.Second),
	)
	h := newHarness(t, testConfig(), f)
	runUntilEmitted(t, h, f)
	got := h.store.kinds()
	if len(got) != 1 || got[0] != domain.KindDisconnect {
		t.Fatalf("incidents = %v, want the open disconnect window", got)
	}
	if h.metrics.connected[ex] {
		t.Error("connected gauge still up")
	}
}

func TestPipelineCountsLateTrades(t *testing.T) {
	f := newFakeFeed(ex, []string{sym},
		connEv(EventConnected, 0),
		tradeEv(1, 0, "1"),
		tradeEv(2, 90*time.Second, "1"), // emits minute 0
		tradeEv(3, 30*time.Second, "1"), // belongs to minute 0: late
	)
	h := newHarness(t, testConfig(), f)
	runUntilEmitted(t, h, f)
	if h.metrics.late != 1 {
		t.Errorf("late trades = %d, want 1", h.metrics.late)
	}
	if h.metrics.incidents[domain.KindOutOfOrder] != 1 {
		t.Errorf("out_of_order incidents = %d, want 1", h.metrics.incidents[domain.KindOutOfOrder])
	}
}

func TestPipelineSinkErrorsAreCountedNotFatal(t *testing.T) {
	f := newFakeFeed(ex, []string{sym},
		connEv(EventConnected, 0),
		tradeEv(1, 0, "1"),
		tradeEv(3, time.Minute, "1"), // gap incident + bar
	)
	h := newHarness(t, testConfig(), f)
	h.store.err = errors.New("disk full")
	h.sink.err = errors.New("disk full")
	runUntilEmitted(t, h, f)
	if h.metrics.sinkErrors[SinkIncidents] != 1 || h.metrics.sinkErrors[SinkBars] != 2 {
		t.Errorf("sink errors = %v", h.metrics.sinkErrors)
	}
}

func TestPipelineUnknownSymbolInheritsConnection(t *testing.T) {
	ev := tradeEv(1, 0, "1")
	ev.Trade.Symbol = "ETHUSDT"
	f := newFakeFeed(ex, []string{sym}, connEv(EventConnected, 0), ev, Event{Kind: 99, Exchange: ex})
	h := newHarness(t, testConfig(), f)
	runUntilEmitted(t, h, f)
	st := h.p.Streams()
	if len(st) != 2 || st[0].Symbol != "BTCUSDT" || st[1].Symbol != "ETHUSDT" || !st[1].Connected || st[1].Trades != 1 {
		t.Errorf("streams = %+v", st)
	}
}

func TestPipelineBackpressureBlocksInsteadOfDropping(t *testing.T) {
	events := []Event{connEv(EventConnected, 0)}
	for id := int64(1); id <= 20; id++ {
		events = append(events, tradeEv(id*2, time.Duration(id)*time.Millisecond, "1")) // every trade is a gap
	}
	f := newFakeFeed(ex, []string{sym}, events...)
	cfg := testConfig()
	cfg.Buffer = 1
	h := newHarness(t, cfg, f)
	h.store.release = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.p.Run(ctx) }()

	time.Sleep(50 * time.Millisecond) // the feed is now blocked on a full buffer
	select {
	case <-f.emitted:
		t.Fatal("feed finished although the pipeline was stalled")
	default:
	}
	close(h.store.release)
	<-f.emitted
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := len(h.store.kinds()); n != 19 {
		t.Errorf("incidents = %d, want 19: nothing may be dropped", n)
	}
	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	if h.metrics.backpressure <= 0 {
		t.Error("backpressure was not measured")
	}
}

func TestPipelineTickFlagsStaleAndFlushesBars(t *testing.T) {
	f := newFakeFeed(ex, []string{sym}, connEv(EventConnected, 0), tradeEv(1, 0, "1"))
	cfg := DefaultConfig()
	cfg.TickInterval = 5 * time.Millisecond
	cfg.StaleAfter = map[domain.StreamKey]time.Duration{{Exchange: ex, Symbol: sym}: time.Second}
	h := newHarness(t, cfg, f) // clock reads t0+1h: stale, and minute 0 is over

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.p.Run(ctx) }()
	deadline := time.After(5 * time.Second)
	for len(h.store.kinds()) == 0 || len(h.sink.written()) == 0 {
		select {
		case <-deadline:
			t.Fatalf("tick did not run: incidents %v, bars %d", h.store.kinds(), len(h.sink.written()))
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := h.store.kinds(); got[0] != domain.KindStaleStream {
		t.Errorf("incidents = %v, want stale_stream", got)
	}
	if n := len(h.sink.written()); n != 1 {
		t.Errorf("bars = %d, want exactly 1 (no double flush at shutdown)", n)
	}
}

func TestPipelineFeedFailureStopsRun(t *testing.T) {
	bad := newFakeFeed(ex, []string{sym})
	bad.err = errors.New("bad symbol")
	good := newFakeFeed(domain.ExchangeKraken, []string{"BTC/USD"})
	h := newHarness(t, testConfig(), bad, good)
	err := h.p.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "feed binance: bad symbol") {
		t.Fatalf("Run() = %v, want the feed error", err)
	}
}

func TestNewPipelineValidates(t *testing.T) {
	ok := Deps{
		Feeds: []Feed{newFakeFeed(ex, nil)}, Incidents: &fakeStore{}, Bars: &fakeSink{},
		Metrics: newFakeMetrics(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := NewPipeline(DefaultConfig(), ok); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	tests := []struct {
		name string
		cfg  func(*Config)
		deps func(*Deps)
	}{
		{"quality", func(c *Config) { c.Quality.SpikeK = 0 }, func(*Deps) {}},
		{"buffer", func(c *Config) { c.Buffer = 0 }, func(*Deps) {}},
		{"tick", func(c *Config) { c.TickInterval = 0 }, func(*Deps) {}},
		{"grace", func(c *Config) { c.BarGrace = -1 }, func(*Deps) {}},
		{"feeds", func(*Config) {}, func(d *Deps) { d.Feeds = nil }},
		{"store", func(*Config) {}, func(d *Deps) { d.Incidents = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, d := DefaultConfig(), ok
			tt.cfg(&cfg)
			tt.deps(&d)
			if _, err := NewPipeline(cfg, d); err == nil {
				t.Error("NewPipeline() = nil error")
			}
		})
	}
}

package quality

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

var (
	t0  = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	key = domain.StreamKey{Exchange: "binance", Symbol: "BTCUSDT"}
)

// trade builds a well-behaved trade: id n, exchange time t0+n ms, received 50 ms later.
func trade(id int64, price string) domain.Trade {
	at := t0.Add(time.Duration(id) * time.Millisecond)
	return domain.Trade{
		Exchange: key.Exchange, Symbol: key.Symbol, TradeID: id,
		Price: domain.MustDecimal(price), Qty: domain.MustDecimal("1"), Side: domain.SideBuy,
		ExchangeTime: at, ReceivedTime: at.Add(50 * time.Millisecond),
	}
}

func connectedStream(cfg Config) *Stream {
	s := NewStream(key, cfg)
	s.Connected(t0)
	return s
}

func kinds(incs []domain.Incident) []domain.IncidentKind {
	out := make([]domain.IncidentKind, 0, len(incs))
	for _, i := range incs {
		out = append(out, i.Kind)
	}
	return out
}

func TestObserveTradeChecks(t *testing.T) {
	tests := []struct {
		name     string
		prev     []domain.Trade
		trade    func() domain.Trade
		want     []domain.IncidentKind
		wantDrop bool
	}{
		{
			name:  "first trade is clean",
			trade: func() domain.Trade { return trade(1, "100") },
		},
		{
			name:  "consecutive ids are clean",
			prev:  []domain.Trade{trade(1, "100")},
			trade: func() domain.Trade { return trade(2, "100") },
		},
		{
			name:  "id jump while connected is a silent gap",
			prev:  []domain.Trade{trade(1, "100")},
			trade: func() domain.Trade { return trade(5, "100") },
			want:  []domain.IncidentKind{domain.KindSequenceGap},
		},
		{
			name:     "repeated id is a duplicate and is dropped",
			prev:     []domain.Trade{trade(1, "100"), trade(2, "100")},
			trade:    func() domain.Trade { return trade(1, "100") },
			want:     []domain.IncidentKind{domain.KindDuplicate},
			wantDrop: true,
		},
		{
			name: "earlier exchange time is out of order",
			prev: []domain.Trade{trade(10, "100")},
			trade: func() domain.Trade {
				tr := trade(11, "100")
				tr.ExchangeTime = t0 // 10 ms before trade 10
				tr.ReceivedTime = t0.Add(20 * time.Millisecond)
				return tr
			},
			want: []domain.IncidentKind{domain.KindOutOfOrder},
		},
		{
			name: "equal exchange time is in order",
			prev: []domain.Trade{trade(10, "100")},
			trade: func() domain.Trade {
				tr := trade(11, "100")
				tr.ExchangeTime = t0.Add(10 * time.Millisecond)
				return tr
			},
		},
		{
			name: "late receipt is clock skew",
			trade: func() domain.Trade {
				tr := trade(1, "100")
				tr.ReceivedTime = tr.ExchangeTime.Add(3 * time.Second)
				return tr
			},
			want: []domain.IncidentKind{domain.KindClockSkew},
		},
		{
			name: "exchange clock ahead of ours is clock skew too",
			trade: func() domain.Trade {
				tr := trade(1, "100")
				tr.ReceivedTime = tr.ExchangeTime.Add(-3 * time.Second)
				return tr
			},
			want: []domain.IncidentKind{domain.KindClockSkew},
		},
		{
			name: "several checks can fire on one trade",
			prev: []domain.Trade{trade(1, "100")},
			trade: func() domain.Trade {
				tr := trade(9, "100")
				tr.ReceivedTime = tr.ExchangeTime.Add(time.Minute)
				return tr
			},
			want: []domain.IncidentKind{domain.KindSequenceGap, domain.KindClockSkew},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := connectedStream(DefaultConfig())
			for _, p := range tt.prev {
				if v := s.Observe(p); len(v.Incidents) != 0 {
					t.Fatalf("setup trade %d raised %v", p.TradeID, kinds(v.Incidents))
				}
			}
			tr := tt.trade()
			v := s.Observe(tr)
			if fmt.Sprint(kinds(v.Incidents)) != fmt.Sprint(tt.want) {
				t.Fatalf("incidents = %v, want %v", kinds(v.Incidents), tt.want)
			}
			if v.Drop != tt.wantDrop {
				t.Errorf("Drop = %v, want %v", v.Drop, tt.wantDrop)
			}
			for _, inc := range v.Incidents {
				if inc.Exchange != key.Exchange || inc.Symbol != key.Symbol || inc.TradeID != tr.TradeID {
					t.Errorf("incident identity = %+v", inc)
				}
				if !inc.DetectedAt.Equal(tr.ReceivedTime) || inc.Detail == "" {
					t.Errorf("incident DetectedAt/Detail = %v / %q", inc.DetectedAt, inc.Detail)
				}
			}
		})
	}
}

func TestSequenceGapWindowAndDetail(t *testing.T) {
	s := connectedStream(DefaultConfig())
	s.Observe(trade(100, "1"))
	v := s.Observe(trade(105, "1"))
	if len(v.Incidents) != 1 {
		t.Fatalf("incidents = %v", kinds(v.Incidents))
	}
	inc := v.Incidents[0]
	if !inc.Start.Equal(trade(100, "1").ExchangeTime) || !inc.End.Equal(trade(105, "1").ExchangeTime) {
		t.Errorf("window = [%v, %v]", inc.Start, inc.End)
	}
	if !strings.Contains(inc.Detail, "4 missing") {
		t.Errorf("detail = %q, want the missing count", inc.Detail)
	}
}

func TestLowerIDDoesNotMoveBaselineBack(t *testing.T) {
	s := connectedStream(DefaultConfig())
	s.Observe(trade(10, "1"))
	s.Observe(trade(12, "1")) // gap of one
	v := s.Observe(trade(11, "1"))
	if got := kinds(v.Incidents); len(got) != 1 || got[0] != domain.KindOutOfOrder {
		t.Fatalf("late fill = %v, want [out_of_order] only", got)
	}
	if v := s.Observe(trade(13, "1")); len(v.Incidents) != 0 {
		t.Errorf("trade 13 after 12 raised %v", kinds(v.Incidents))
	}
}

func TestOutOfOrderTolerance(t *testing.T) {
	cfg := DefaultConfig()
	cfg.OutOfOrderTolerance = 5 * time.Millisecond
	s := connectedStream(cfg)
	s.Observe(trade(10, "1"))

	within := trade(11, "1")
	within.ExchangeTime = trade(10, "1").ExchangeTime.Add(-5 * time.Millisecond)
	if v := s.Observe(within); len(v.Incidents) != 0 {
		t.Errorf("5ms back with 5ms tolerance raised %v", kinds(v.Incidents))
	}
	beyond := trade(12, "1")
	beyond.ExchangeTime = trade(10, "1").ExchangeTime.Add(-6 * time.Millisecond)
	if v := s.Observe(beyond); len(v.Incidents) != 1 || v.Incidents[0].Kind != domain.KindOutOfOrder {
		t.Errorf("6ms back raised %v, want out_of_order", kinds(v.Incidents))
	}
}

func TestDuplicateWindowEvicts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DuplicateWindow = 3
	s := connectedStream(cfg)
	for id := int64(1); id <= 4; id++ { // window now holds 2,3,4
		s.Observe(trade(id, "1"))
	}
	if v := s.Observe(trade(2, "1")); !v.Drop {
		t.Error("id 2 is inside the window and should be a duplicate")
	}
	if v := s.Observe(trade(1, "1")); v.Drop {
		t.Error("id 1 was evicted and should not be reported as a duplicate")
	}
}

func TestDuplicatesSurviveReconnect(t *testing.T) {
	s := connectedStream(DefaultConfig())
	s.Observe(trade(1, "1"))
	s.Disconnected(t0.Add(time.Second))
	s.Connected(t0.Add(2 * time.Second))
	if v := s.Observe(trade(1, "1")); !v.Drop {
		t.Error("a replayed trade after reconnect must still be a duplicate")
	}
}

func TestPriceSpike(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SpikeWindow, cfg.SpikeMinSamples, cfg.SpikeK, cfg.SpikeMinDeviation = 11, 5, 5, 0.001

	warm := func(s *Stream) {
		// prices oscillate 99.9..100.1: median 100, MAD 0.1
		for i, p := range []string{"100", "100.1", "99.9", "100", "100.1", "99.9", "100"} {
			s.Observe(trade(int64(i+1), p))
		}
	}
	tests := []struct {
		name  string
		warm  bool
		price string
		spike bool
	}{
		{"not enough samples", false, "500", false},
		{"inside k*MAD", true, "100.5", false},
		{"beyond k*MAD", true, "101", true},
		{"downward spike", true, "90", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := connectedStream(cfg)
			if tt.warm {
				warm(s)
			}
			v := s.Observe(trade(100, tt.price))
			got := false
			for _, inc := range v.Incidents {
				if inc.Kind == domain.KindPriceSpike {
					got = true
				}
			}
			if got != tt.spike {
				t.Errorf("spike = %v, want %v (incidents %v)", got, tt.spike, kinds(v.Incidents))
			}
			if v.Drop {
				t.Error("a spike is flagged, never dropped")
			}
		})
	}
}

func TestPriceSpikeFloorWhenMADIsZero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SpikeWindow, cfg.SpikeMinSamples, cfg.SpikeMinDeviation = 5, 3, 0.01
	s := connectedStream(cfg)
	for id := int64(1); id <= 6; id++ { // also wraps the ring buffer
		s.Observe(trade(id, "100"))
	}
	if v := s.Observe(trade(7, "100.5")); len(v.Incidents) != 0 {
		t.Errorf("0.5%% move with 1%% floor raised %v", kinds(v.Incidents))
	}
	if v := s.Observe(trade(8, "102")); len(v.Incidents) != 1 || v.Incidents[0].Kind != domain.KindPriceSpike {
		t.Errorf("2%% move with 1%% floor raised %v, want price_spike", kinds(v.Incidents))
	}
}

func TestKnownGapIsNotASilentGap(t *testing.T) {
	s := NewStream(key, DefaultConfig())
	if _, ok := s.Connected(t0); ok {
		t.Fatal("first connect must not report a disconnect")
	}
	if !s.IsConnected() {
		t.Fatal("Connected() = false after connect")
	}
	s.Observe(trade(1, "100"))

	down, up := t0.Add(time.Second), t0.Add(5*time.Second)
	s.Disconnected(down)
	s.Disconnected(down.Add(time.Second)) // repeated notice keeps the first timestamp
	if s.IsConnected() {
		t.Fatal("Connected() = true after disconnect")
	}
	inc, ok := s.Connected(up)
	if !ok {
		t.Fatal("reconnect did not report the known gap")
	}
	if inc.Kind != domain.KindDisconnect || !inc.Start.Equal(down) || !inc.End.Equal(up) || !inc.DetectedAt.Equal(up) {
		t.Errorf("disconnect incident = %+v", inc)
	}
	if !strings.Contains(inc.Detail, "last trade_id before the gap: 1") {
		t.Errorf("detail = %q", inc.Detail)
	}

	// Trades 2..49 were missed while down: a known gap, not a sequence_gap.
	if v := s.Observe(trade(50, "100")); len(v.Incidents) != 0 {
		t.Errorf("first trade after reconnect raised %v", kinds(v.Incidents))
	}
	// But a jump after that is silent again.
	if v := s.Observe(trade(52, "100")); len(v.Incidents) != 1 || v.Incidents[0].Kind != domain.KindSequenceGap {
		t.Errorf("jump after reconnect raised %v, want sequence_gap", kinds(v.Incidents))
	}
	if _, ok := s.Connected(up.Add(time.Second)); ok {
		t.Error("Connected while already connected must not report a gap")
	}
}

func TestOpenGap(t *testing.T) {
	s := NewStream(key, DefaultConfig())
	if _, ok := s.OpenGap(t0); ok {
		t.Error("never-connected stream has no open gap")
	}
	s.Connected(t0)
	if _, ok := s.OpenGap(t0); ok {
		t.Error("connected stream has no open gap")
	}
	s.Disconnected(t0.Add(time.Second))
	inc, ok := s.OpenGap(t0.Add(3 * time.Second))
	if !ok || inc.Kind != domain.KindDisconnect || !inc.Start.Equal(t0.Add(time.Second)) || !inc.End.Equal(t0.Add(3*time.Second)) {
		t.Errorf("OpenGap = %+v, %v", inc, ok)
	}
}

func TestCheckStale(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StaleAfter = 10 * time.Second
	s := NewStream(key, cfg)

	if _, ok := s.CheckStale(t0.Add(time.Hour)); ok {
		t.Error("never-connected stream must not be stale")
	}
	s.Connected(t0)
	if _, ok := s.CheckStale(t0.Add(10 * time.Second)); ok {
		t.Error("exactly at the threshold is not stale")
	}
	inc, ok := s.CheckStale(t0.Add(11 * time.Second))
	if !ok || inc.Kind != domain.KindStaleStream || !inc.Start.Equal(t0) || !inc.End.Equal(t0.Add(11*time.Second)) {
		t.Fatalf("CheckStale = %+v, %v", inc, ok)
	}
	if _, ok := s.CheckStale(t0.Add(20 * time.Second)); ok {
		t.Error("a stale episode is reported once")
	}

	tr := trade(1, "1")
	tr.ExchangeTime, tr.ReceivedTime = t0.Add(30*time.Second), t0.Add(30*time.Second)
	s.Observe(tr) // re-arms
	if _, ok := s.CheckStale(t0.Add(41 * time.Second)); !ok {
		t.Error("stale should re-arm after a trade")
	}

	s.Disconnected(t0.Add(50 * time.Second))
	if _, ok := s.CheckStale(t0.Add(time.Hour)); ok {
		t.Error("a disconnected stream is never stale")
	}
}

func TestConfigValidate(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig invalid: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{"clock skew", func(c *Config) { c.ClockSkew = 0 }},
		{"tolerance", func(c *Config) { c.OutOfOrderTolerance = -1 }},
		{"stale", func(c *Config) { c.StaleAfter = 0 }},
		{"dup window", func(c *Config) { c.DuplicateWindow = 0 }},
		{"spike window", func(c *Config) { c.SpikeWindow = 2 }},
		{"min samples low", func(c *Config) { c.SpikeMinSamples = 2 }},
		{"min samples above window", func(c *Config) { c.SpikeMinSamples = c.SpikeWindow + 1 }},
		{"k", func(c *Config) { c.SpikeK = 0 }},
		{"min deviation", func(c *Config) { c.SpikeMinDeviation = -0.1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfig()
			tt.mutate(&c)
			if c.Validate() == nil {
				t.Error("Validate() = nil, want error")
			}
		})
	}
}

func TestMedianMAD(t *testing.T) {
	tests := []struct {
		xs          []float64
		median, mad float64
	}{
		{[]float64{1}, 1, 0},
		{[]float64{3, 1, 2}, 2, 1},
		{[]float64{1, 2, 3, 4}, 2.5, 1},
		{[]float64{1, 1, 1, 100}, 1, 0},
	}
	for _, tt := range tests {
		m, d := medianMAD(tt.xs, nil)
		if math.Abs(m-tt.median) > 1e-12 || math.Abs(d-tt.mad) > 1e-12 {
			t.Errorf("medianMAD(%v) = %v, %v; want %v, %v", tt.xs, m, d, tt.median, tt.mad)
		}
	}
}

func TestKey(t *testing.T) {
	if NewStream(key, DefaultConfig()).Key() != key {
		t.Error("Key() mismatch")
	}
}

// block builds a KleverChain-like block: nonce n, a block every 4 s, received 500 ms later.
func block(nonce int64) domain.Block {
	at := t0.Add(time.Duration(nonce) * 4 * time.Second)
	return domain.Block{
		Chain: "klever", Network: "testnet", Nonce: nonce, Hash: fmt.Sprintf("h%d", nonce),
		ChainTime: at, ReceivedTime: at.Add(500 * time.Millisecond),
	}
}

// TestBlockStream runs the gap story of docs/adr/0002 over block nonces: the
// rules are the trade rules, the wording names nonces and blocks.
func TestBlockStream(t *testing.T) {
	bkey := domain.StreamKey{Exchange: "klever", Symbol: "testnet"}
	s := NewBlockStream(bkey, DefaultConfig())
	s.Connected(t0)

	for n := int64(1); n <= 3; n++ {
		if v := s.ObserveBlock(block(n)); len(v.Incidents) != 0 || v.Drop {
			t.Fatalf("block %d: %+v, want clean", n, v)
		}
	}

	v := s.ObserveBlock(block(6))
	if got := kinds(v.Incidents); len(got) != 1 || got[0] != domain.KindSequenceGap {
		t.Fatalf("nonce jump: %v, want sequence_gap", got)
	}
	gap := v.Incidents[0]
	if gap.Detail != "nonce jumped from 3 to 6 while connected (2 missing)" || gap.TradeID != 6 ||
		!gap.Start.Equal(block(3).ChainTime) || !gap.End.Equal(block(6).ChainTime) || gap.Key() != bkey {
		t.Errorf("gap = %+v", gap)
	}

	v = s.ObserveBlock(block(6))
	if got := kinds(v.Incidents); !v.Drop || len(got) != 1 || got[0] != domain.KindDuplicate ||
		v.Incidents[0].Detail != "nonce 6 already seen in the last 10000 blocks" {
		t.Fatalf("replayed block: %+v, want a dropped duplicate", v)
	}

	late := block(7)
	late.ReceivedTime = late.ChainTime.Add(3 * time.Second)
	if got := kinds(s.ObserveBlock(late).Incidents); len(got) != 1 || got[0] != domain.KindClockSkew {
		t.Errorf("late block: %v, want clock_skew", got)
	}

	// Disconnected while blocks 8..9 were produced: a known gap, not a silent one.
	s.Disconnected(block(7).ReceivedTime.Add(time.Second))
	inc, ok := s.Connected(block(10).ChainTime)
	if !ok || inc.Kind != domain.KindDisconnect || !strings.Contains(inc.Detail, "last nonce before the gap: 7") {
		t.Fatalf("reconnect: %+v %v, want a disconnect incident", inc, ok)
	}
	if v := s.ObserveBlock(block(10)); len(v.Incidents) != 0 {
		t.Errorf("first block after reconnect: %v, want clean", kinds(v.Incidents))
	}

	stale, ok := s.CheckStale(block(10).ReceivedTime.Add(61 * time.Second))
	if !ok || stale.Kind != domain.KindStaleStream || !strings.HasPrefix(stale.Detail, "no block for 1m1s") {
		t.Errorf("stale = %+v %v", stale, ok)
	}
}

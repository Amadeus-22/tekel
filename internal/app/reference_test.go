package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/reference"
)

type fakeRefSource struct {
	calls  []string // convert:bases
	prices map[string]map[string]string
	fail   map[string]bool
	at     time.Time
}

func (f *fakeRefSource) Quotes(_ context.Context, convert string, bases []string) (map[string]reference.Quote, error) {
	f.calls = append(f.calls, convert+":"+strings.Join(bases, ","))
	if f.fail[convert] {
		return nil, errors.New("boom")
	}
	out := map[string]reference.Quote{}
	for _, b := range bases {
		if p, ok := f.prices[convert][b]; ok {
			out[b] = reference.Quote{Price: domain.MustDecimal(p), UpdatedAt: f.at}
		}
	}
	return out, nil
}

type fakeLister []StreamStatus

func (f fakeLister) Streams() []StreamStatus { return f }

type fakeRecorder struct {
	mu  sync.Mutex
	got []domain.Incident
}

func (f *fakeRecorder) Record(_ context.Context, inc domain.Incident) {
	f.mu.Lock()
	f.got = append(f.got, inc)
	f.mu.Unlock()
}

type fakeRefMetrics struct {
	ok, failed int
	usage      []reference.Usage
	devs       map[string]float64
}

func (f *fakeRefMetrics) ReferenceFetch(ok bool) {
	if ok {
		f.ok++
	} else {
		f.failed++
	}
}

func (f *fakeRefMetrics) ReferenceDeviation(exchange, symbol string, d float64) {
	f.devs[exchange+":"+symbol] = d
}

func (f *fakeRefMetrics) ReferenceUsage(u reference.Usage) { f.usage = append(f.usage, u) }

// usageSource is a reference source that also reports its credit usage.
type usageSource struct {
	fakeRefSource
	usage reference.Usage
	err   error
}

func (u *usageSource) Usage(context.Context) (reference.Usage, error) { return u.usage, u.err }

func TestReferenceCheck(t *testing.T) {
	now := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	trade := func(ex, sym, price string) StreamStatus {
		return StreamStatus{Exchange: ex, Symbol: sym, Kind: domain.StreamTrades, LastPrice: price, LastReceivedTime: now}
	}
	streams := fakeLister{
		trade("binance", "BTCUSDT", "100.2"),
		trade("kraken", "BTC/USD", "103"), // 3% off: incident
		trade("binance", "ETHUSDT", "50"), // no quote for ETH
		trade("mercadobitcoin", "BTC-BRL", "500"),
		{Exchange: "klever", Symbol: "mainnet", Kind: domain.StreamBlocks, LastPrice: "", LastReceivedTime: now},
		{Exchange: "kraken", Symbol: "ETH/USD", Kind: domain.StreamTrades}, // never traded
	}
	src := &fakeRefSource{prices: map[string]map[string]string{"USD": {"BTC": "100"}}, fail: map[string]bool{"BRL": true}, at: now}
	rec := &fakeRecorder{}
	met := &fakeRefMetrics{devs: map[string]float64{}}
	rc, err := NewReferenceCheck(ReferenceConfig{Monitor: reference.DefaultConfig(), Interval: time.Minute}, ReferenceDeps{
		Source: src, Streams: streams, Recorder: rec, Metrics: met,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	rc.Check(context.Background())

	if want := []string{"BRL:BTC", "USD:BTC,ETH"}; strings.Join(src.calls, " ") != strings.Join(want, " ") {
		t.Errorf("calls = %v, want one per currency %v", src.calls, want)
	}
	if met.ok != 1 || met.failed != 1 {
		t.Errorf("fetch metrics ok=%d failed=%d, want 1 and 1", met.ok, met.failed)
	}
	if len(rec.got) != 1 || rec.got[0].Symbol != "BTC/USD" || rec.got[0].Kind != domain.KindPriceDeviation {
		t.Fatalf("incidents = %+v, want one price_deviation on kraken BTC/USD", rec.got)
	}
	refs := rc.References()
	if len(refs) != 2 {
		t.Fatalf("references = %v, want binance BTCUSDT and kraken BTC/USD", refs)
	}
	if d := met.devs["binance:BTCUSDT"]; d < 0.0019 || d > 0.0021 {
		t.Errorf("binance deviation = %v, want 0.002", d)
	}

	rc.Check(context.Background()) // same prices: no repeat incident
	if len(rec.got) != 1 {
		t.Errorf("second check recorded %d incidents in total, want still 1", len(rec.got))
	}
}

func TestNewReferenceCheckValidates(t *testing.T) {
	if _, err := NewReferenceCheck(ReferenceConfig{}, ReferenceDeps{}); err == nil {
		t.Fatal("want an error for empty config and deps")
	}
}

func TestReferenceCheckRunStopsOnCancel(t *testing.T) {
	src := &fakeRefSource{}
	rc, err := NewReferenceCheck(ReferenceConfig{Monitor: reference.DefaultConfig(), Interval: time.Millisecond}, ReferenceDeps{
		Source: src, Streams: fakeLister{}, Recorder: &fakeRecorder{}, Metrics: &fakeRefMetrics{devs: map[string]float64{}},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := rc.Run(ctx); err != nil {
		t.Fatalf("Run = %v, want nil on cancel", err)
	}
}

func TestReferenceCheckRunChecksSoonAfterStart(t *testing.T) {
	src := &fakeRefSource{}
	rc, err := NewReferenceCheck(ReferenceConfig{Monitor: reference.DefaultConfig(), Interval: time.Hour, FirstDelay: time.Millisecond},
		ReferenceDeps{
			Source: src, Streams: fakeLister{{Exchange: "binance", Symbol: "BTCUSDT", Kind: domain.StreamTrades, LastPrice: "1"}},
			Recorder: &fakeRecorder{}, Metrics: &fakeRefMetrics{devs: map[string]float64{}},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = rc.Run(ctx)
	if len(src.calls) != 1 {
		t.Fatalf("calls = %v, want exactly one check before the hour-long interval", src.calls)
	}
}

func TestReferenceCheckReportsUsageWhenTheSourceCan(t *testing.T) {
	newCheck := func(src ReferenceSource, met *fakeRefMetrics) *ReferenceCheck {
		rc, err := NewReferenceCheck(ReferenceConfig{Monitor: reference.DefaultConfig(), Interval: time.Minute}, ReferenceDeps{
			Source: src, Streams: fakeLister{}, Recorder: &fakeRecorder{}, Metrics: met,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		return rc
	}

	met := &fakeRefMetrics{devs: map[string]float64{}}
	want := reference.Usage{UsedToday: 3, UsedMonth: 40, LeftMonth: 9960}
	newCheck(&usageSource{usage: want}, met).Check(context.Background())
	if len(met.usage) != 1 || met.usage[0] != want {
		t.Errorf("usage = %v, want [%v]", met.usage, want)
	}

	met = &fakeRefMetrics{devs: map[string]float64{}}
	newCheck(&usageSource{err: errors.New("boom")}, met).Check(context.Background())
	if len(met.usage) != 0 {
		t.Errorf("usage after a failed read = %v, want none", met.usage)
	}

	met = &fakeRefMetrics{devs: map[string]float64{}}
	newCheck(&fakeRefSource{}, met).Check(context.Background())
	if len(met.usage) != 0 {
		t.Errorf("usage from a source without Usage = %v, want none", met.usage)
	}
}

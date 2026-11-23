package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/reference"
)

// ReferenceSource returns reference prices for bases in one currency, keyed by
// base ("BTC"). The CoinMarketCap adapter implements it.
type ReferenceSource interface {
	Quotes(ctx context.Context, convert string, bases []string) (map[string]reference.Quote, error)
}

// StreamLister reports live stream state; Pipeline implements it.
type StreamLister interface {
	Streams() []StreamStatus
}

// IncidentRecorder records an incident; Pipeline implements it.
type IncidentRecorder interface {
	Record(ctx context.Context, inc domain.Incident)
}

// ReferenceMetrics receives the reference check's measurements.
type ReferenceMetrics interface {
	ReferenceFetch(ok bool)
	ReferenceDeviation(exchange, symbol string, deviation float64)
}

// ReferenceConfig tunes the reference check.
type ReferenceConfig struct {
	Monitor reference.Config
	// Interval is how often reference prices are fetched. Each run costs one
	// API call per quote currency in use (USD, BRL…).
	Interval time.Duration
	// FirstDelay is the wait before the first check: long enough for the
	// feeds to see a trade. Capped at Interval.
	FirstDelay time.Duration
}

// ReferenceDeps are the reference check's collaborators.
type ReferenceDeps struct {
	Source   ReferenceSource
	Streams  StreamLister
	Recorder IncidentRecorder
	Metrics  ReferenceMetrics
	Logger   *slog.Logger
	// Now is the clock; nil means time.Now.
	Now func() time.Time
}

// ReferenceCheck compares every trade stream with a reference price on a timer.
type ReferenceCheck struct {
	cfg     ReferenceConfig
	d       ReferenceDeps
	monitor *reference.Monitor

	mu      sync.RWMutex
	results map[domain.StreamKey]reference.Result
}

// NewReferenceCheck validates cfg and deps.
func NewReferenceCheck(cfg ReferenceConfig, d ReferenceDeps) (*ReferenceCheck, error) {
	var errs []error
	m, err := reference.NewMonitor(cfg.Monitor)
	if err != nil {
		errs = append(errs, err)
	}
	if cfg.Interval <= 0 {
		errs = append(errs, errors.New("reference interval must be positive"))
	}
	if d.Source == nil || d.Streams == nil || d.Recorder == nil || d.Metrics == nil || d.Logger == nil {
		errs = append(errs, errors.New("reference source, streams, recorder, metrics and logger are required"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("new reference check: %w", err)
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	return &ReferenceCheck{cfg: cfg, d: d, monitor: m, results: map[domain.StreamKey]reference.Result{}}, nil
}

// Run checks once per interval until ctx ends. A failed fetch is logged and
// retried on the next tick; it never stops Tekel. The first check runs after
// FirstDelay, so a fresh start shows reference prices in seconds, not after a
// whole interval (which can be minutes on a free API plan).
func (r *ReferenceCheck) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(min(r.cfg.FirstDelay, r.cfg.Interval)):
		r.Check(ctx)
	}
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.Check(ctx)
		}
	}
}

// Check runs one comparison: read the streams, fetch the quotes they need,
// record new incidents and keep the results for the API.
func (r *ReferenceCheck) Check(ctx context.Context) {
	var obs []reference.Observation
	need := map[string][]string{} // convert → bases
	for _, s := range r.d.Streams.Streams() {
		if s.Kind != domain.StreamTrades || s.LastPrice == "" {
			continue
		}
		asset, ok := reference.AssetFor(s.Symbol)
		if !ok {
			continue
		}
		price, err := domain.ParseDecimal(s.LastPrice)
		if err != nil {
			continue
		}
		obs = append(obs, reference.Observation{
			Key: domain.StreamKey{Exchange: s.Exchange, Symbol: s.Symbol}, Price: price, ReceivedAt: s.LastReceivedTime,
		})
		if !slices.Contains(need[asset.Convert], asset.Base) {
			need[asset.Convert] = append(need[asset.Convert], asset.Base)
		}
	}
	quotes := map[reference.Asset]reference.Quote{}
	for _, convert := range slices.Sorted(maps.Keys(need)) {
		bases := need[convert]
		slices.Sort(bases)
		got, err := r.d.Source.Quotes(ctx, convert, bases)
		r.d.Metrics.ReferenceFetch(err == nil)
		if err != nil {
			r.d.Logger.Warn("reference quotes", "convert", convert, "error", err)
			continue
		}
		for base, q := range got {
			quotes[reference.Asset{Base: base, Convert: convert}] = q
		}
	}
	results, incidents := r.monitor.Check(r.d.Now(), obs, quotes)
	r.mu.Lock()
	for _, res := range results {
		r.results[res.Key] = res
	}
	r.mu.Unlock()
	for _, res := range results {
		r.d.Metrics.ReferenceDeviation(res.Key.Exchange, res.Key.Symbol, res.Deviation)
	}
	for _, inc := range incidents {
		r.d.Recorder.Record(ctx, inc)
	}
}

// References returns the latest result per stream. Safe from any goroutine.
func (r *ReferenceCheck) References() map[domain.StreamKey]reference.Result {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.results)
}

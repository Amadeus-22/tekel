// Package reference compares each stream's last trade with an external
// reference price (CoinMarketCap) and raises a price_deviation incident when
// the two drift apart. It is pure: the caller supplies the clock, the stream
// prices and the reference quotes.
package reference

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
)

// quoteAssets are the quote currencies recognised at the end of a symbol with
// no separator ("BTCUSDT"). Longer names first, so "USDT" wins over "USD".
var quoteAssets = []string{"FDUSD", "USDT", "USDC", "BUSD", "USD", "BRL", "EUR", "BTC", "ETH"}

// SplitPair splits an exchange symbol into base and quote asset. It understands
// "BTC/USD" (Kraken), "BTC-BRL" (Mercado Bitcoin) and "BTCUSDT" (Binance).
func SplitPair(symbol string) (base, quote string, ok bool) {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	for _, sep := range []string{"/", "-"} {
		if b, q, found := strings.Cut(s, sep); found {
			if b == "" || q == "" {
				return "", "", false
			}
			return b, q, true
		}
	}
	for _, q := range quoteAssets {
		if b, found := strings.CutSuffix(s, q); found && b != "" {
			return b, q, true
		}
	}
	return "", "", false
}

// Currency is the fiat currency a reference quote is asked in for a quote
// asset: dollar stablecoins compare against USD, anything else against itself.
func Currency(quote string) string {
	switch quote {
	case "USDT", "USDC", "BUSD", "FDUSD", "USD":
		return "USD"
	}
	return quote
}

// Asset is one reference price to fetch: Base priced in Convert ("BTC" in "USD").
type Asset struct {
	Base, Convert string
}

func (a Asset) String() string { return a.Base + "/" + a.Convert }

// AssetFor returns the reference asset of an exchange symbol.
func AssetFor(symbol string) (Asset, bool) {
	base, quote, ok := SplitPair(symbol)
	if !ok {
		return Asset{}, false
	}
	return Asset{Base: base, Convert: Currency(quote)}, true
}

// Quote is one reference price and when the provider last updated it.
type Quote struct {
	Price     domain.Decimal
	UpdatedAt time.Time
}

// Observation is a stream's last trade.
type Observation struct {
	Key        domain.StreamKey
	Price      domain.Decimal
	ReceivedAt time.Time
}

// Config tunes the check.
type Config struct {
	// Threshold is the relative deviation that raises an incident (0.01 = 1%).
	Threshold float64
	// MaxQuoteAge skips a reference quote older than this.
	MaxQuoteAge time.Duration
	// MaxTradeAge skips a stream whose last trade is older than this; silence
	// is the stale_stream check's job, not this one.
	MaxTradeAge time.Duration
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{Threshold: 0.01, MaxQuoteAge: 10 * time.Minute, MaxTradeAge: 2 * time.Minute}
}

// Validate reports every invalid field.
func (c Config) Validate() error {
	var errs []error
	if !(c.Threshold > 0 && c.Threshold < 1) {
		errs = append(errs, fmt.Errorf("threshold must be between 0 and 1, got %v", c.Threshold))
	}
	if c.MaxQuoteAge <= 0 || c.MaxTradeAge <= 0 {
		errs = append(errs, errors.New("max quote age and max trade age must be positive"))
	}
	return errors.Join(errs...)
}

// Result is the latest comparison for one stream.
type Result struct {
	Key       domain.StreamKey
	Asset     Asset
	Reference domain.Decimal
	// Deviation is (last trade − reference) / reference: +0.002 is 0.2% above.
	Deviation float64
	CheckedAt time.Time
}

// Monitor keeps which streams are already out of range, so a lasting deviation
// raises one incident, not one per check. A stream is back in range when its
// deviation falls under half the threshold (hysteresis: no flapping at the edge).
type Monitor struct {
	cfg      Config
	breached map[domain.StreamKey]bool
}

// NewMonitor validates cfg.
func NewMonitor(cfg Config) (*Monitor, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("reference config: %w", err)
	}
	return &Monitor{cfg: cfg, breached: map[domain.StreamKey]bool{}}, nil
}

// Check compares every observation that has a fresh trade and a fresh quote.
// It returns one result per compared stream and the incidents to record.
func (m *Monitor) Check(now time.Time, obs []Observation, quotes map[Asset]Quote) ([]Result, []domain.Incident) {
	var results []Result
	var incidents []domain.Incident
	for _, o := range obs {
		asset, ok := AssetFor(o.Key.Symbol)
		if !ok || o.Price.IsZero() || now.Sub(o.ReceivedAt) > m.cfg.MaxTradeAge {
			continue
		}
		q, ok := quotes[asset]
		if !ok || q.Price.IsZero() || now.Sub(q.UpdatedAt) > m.cfg.MaxQuoteAge {
			continue
		}
		dev := Deviation(o.Price, q.Price)
		results = append(results, Result{Key: o.Key, Asset: asset, Reference: q.Price, Deviation: dev, CheckedAt: now})
		switch abs := math.Abs(dev); {
		case abs >= m.cfg.Threshold && !m.breached[o.Key]:
			m.breached[o.Key] = true
			incidents = append(incidents, domain.Incident{
				Exchange: o.Key.Exchange, Symbol: o.Key.Symbol, Kind: domain.KindPriceDeviation,
				DetectedAt: now,
				Detail: fmt.Sprintf("last trade %s vs CoinMarketCap %s %s (%+.2f%%, threshold %.2f%%)",
					o.Price, domain.FormatRat(q.Price.Rat()), asset.Convert, dev*100, m.cfg.Threshold*100),
			})
		case abs < m.cfg.Threshold/2:
			delete(m.breached, o.Key)
		}
	}
	return results, incidents
}

// Deviation returns (price − ref) / ref, computed exactly and rounded once.
func Deviation(price, ref domain.Decimal) float64 {
	r := ref.Rat()
	d := new(big.Rat).Sub(price.Rat(), r)
	f, _ := d.Quo(d, r).Float64()
	return f
}

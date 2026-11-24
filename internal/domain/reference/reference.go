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

// AssetsFor returns every reference price needed to price symbol: its base in
// the reference currency and, when the symbol is quoted in a dollar stablecoin
// ("BTCUSDT"), the stablecoin itself. The trade is then compared with the base
// priced in that stablecoin, so the stablecoin's own distance from the dollar
// (USDT trades near, not at, 1 USD) does not read as a deviation.
func AssetsFor(symbol string) []Asset {
	asset, ok := AssetFor(symbol)
	if !ok {
		return nil
	}
	out := []Asset{asset}
	if _, quote, _ := SplitPair(symbol); quote != asset.Convert {
		out = append(out, Asset{Base: quote, Convert: asset.Convert})
	}
	return out
}

// Quote is one reference price and when the provider last updated it.
type Quote struct {
	Price     domain.Decimal
	UpdatedAt time.Time
}

// Usage is the API credit consumption the provider reports for the key.
type Usage struct {
	UsedToday, UsedMonth, LeftMonth int
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
		if o.Price.IsZero() || now.Sub(o.ReceivedAt) > m.cfg.MaxTradeAge {
			continue
		}
		asset, ref, ok := m.reference(now, o.Key.Symbol, quotes)
		if !ok {
			continue
		}
		dev := Deviation(o.Price, ref)
		results = append(results, Result{Key: o.Key, Asset: asset, Reference: ref, Deviation: dev, CheckedAt: now})
		switch abs := math.Abs(dev); {
		case abs >= m.cfg.Threshold && !m.breached[o.Key]:
			m.breached[o.Key] = true
			incidents = append(incidents, domain.Incident{
				Exchange: o.Key.Exchange, Symbol: o.Key.Symbol, Kind: domain.KindPriceDeviation,
				DetectedAt: now,
				Detail: fmt.Sprintf("last trade %s vs CoinMarketCap %s %s (%+.2f%%, threshold %.2f%%)",
					o.Price, ref, asset.Convert, dev*100, m.cfg.Threshold*100),
			})
		case abs < m.cfg.Threshold/2:
			delete(m.breached, o.Key)
		}
	}
	return results, incidents
}

// reference returns the price to compare symbol's trades with, and the asset it
// is expressed in: the base in the reference currency, or, for a stablecoin
// quote, the base divided by the stablecoin (both from the same provider). It
// reports false when a quote is missing, zero or older than MaxQuoteAge.
func (m *Monitor) reference(now time.Time, symbol string, quotes map[Asset]Quote) (Asset, domain.Decimal, bool) {
	assets := AssetsFor(symbol)
	if len(assets) == 0 {
		return Asset{}, domain.Decimal{}, false
	}
	prices := make([]*big.Rat, len(assets))
	for i, a := range assets {
		q, ok := quotes[a]
		if !ok || q.Price.IsZero() || now.Sub(q.UpdatedAt) > m.cfg.MaxQuoteAge {
			return Asset{}, domain.Decimal{}, false
		}
		prices[i] = q.Price.Rat()
	}
	if len(assets) == 1 {
		return assets[0], quotes[assets[0]].Price, true
	}
	// Base in the stablecoin: (base in USD) / (stablecoin in USD).
	cross, err := domain.ParseDecimal(domain.FormatRat(new(big.Rat).Quo(prices[0], prices[1])))
	if err != nil {
		return Asset{}, domain.Decimal{}, false
	}
	return Asset{Base: assets[0].Base, Convert: assets[1].Base}, cross, true
}

// Deviation returns (price − ref) / ref, computed exactly and rounded once.
func Deviation(price, ref domain.Decimal) float64 {
	r := ref.Rat()
	d := new(big.Rat).Sub(price.Rat(), r)
	f, _ := d.Quo(d, r).Float64()
	return f
}

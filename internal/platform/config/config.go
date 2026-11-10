// Package config reads tickguard's configuration from environment variables,
// once, at startup. Every variable is documented in the README table.
package config

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Amadeus-22/tickguard/internal/domain"
)

// Config is the validated process configuration. It holds plain values (and
// domain stream keys); cmd/tickguard maps them onto the packages that use them.
type Config struct {
	HTTPAddr        string
	LogLevel        string
	DataDir         string
	DBPath          string
	ShutdownTimeout time.Duration

	Binance Feed
	Kraken  Feed
	// MercadoBitcoin is off unless symbols are set: LEAN needs a custom market
	// to read its output (docs/adr/0006).
	MercadoBitcoin Feed
	// Klever's one "symbol" is the network ("mainnet" or "testnet"); none
	// turns the KleverChain block feed off. An empty URL means the public
	// testnet node (docs/adr/0007).
	Klever Feed

	ClockSkew           time.Duration
	OutOfOrderTolerance time.Duration
	StaleAfter          time.Duration
	// StaleAfterOverrides holds per-stream stale thresholds.
	StaleAfterOverrides map[domain.StreamKey]time.Duration
	DuplicateWindow     int
	SpikeWindow         int
	SpikeK              float64
	SpikeMinDeviation   float64

	Buffer   int
	BarGrace time.Duration
}

// Feed is one source's endpoint and what to subscribe to there. An empty
// Symbols list turns the source off.
type Feed struct {
	URL     string
	Symbols []string
}

// On reports whether the feed is configured to run.
func (f Feed) On() bool { return len(f.Symbols) > 0 }

// Feeds lists every source in a fixed order: Binance, Kraken, Mercado
// Bitcoin, Klever.
func (c Config) Feeds() []Feed { return []Feed{c.Binance, c.Kraken, c.MercadoBitcoin, c.Klever} }

// Lookup is os.LookupEnv's signature, injectable for tests.
type Lookup func(key string) (string, bool)

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }

// Load reads every variable through lookup and reports all problems at once, so
// a misconfigured deployment fails with the full list rather than one at a time.
func Load(lookup Lookup) (Config, error) {
	p := parser{lookup: lookup}
	c := Config{
		HTTPAddr:        p.str("TICKGUARD_HTTP_ADDR", "127.0.0.1:8080"),
		LogLevel:        p.str("TICKGUARD_LOG_LEVEL", "info"),
		DataDir:         p.str("TICKGUARD_DATA_DIR", "./data"),
		DBPath:          p.str("TICKGUARD_DB_PATH", "./tickguard.db"),
		ShutdownTimeout: p.duration("TICKGUARD_SHUTDOWN_TIMEOUT", 10*time.Second),

		Binance: p.feed("TICKGUARD_BINANCE_URL", "wss://stream.binance.com:9443",
			"TICKGUARD_BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT"),
		Kraken: p.feed("TICKGUARD_KRAKEN_URL", "wss://ws.kraken.com/v2",
			"TICKGUARD_KRAKEN_SYMBOLS", "BTC/USD,ETH/USD"),
		MercadoBitcoin: p.feed("TICKGUARD_MERCADOBITCOIN_URL", "wss://ws.mercadobitcoin.net/ws",
			"TICKGUARD_MERCADOBITCOIN_SYMBOLS", ""),
		Klever: p.feed("TICKGUARD_KLEVER_URL", "", "TICKGUARD_KLEVER_NETWORK", ""),

		ClockSkew:           p.duration("TICKGUARD_CLOCK_SKEW", 2*time.Second),
		OutOfOrderTolerance: p.duration("TICKGUARD_OUT_OF_ORDER_TOLERANCE", 0),
		StaleAfter:          p.duration("TICKGUARD_STALE_AFTER", 60*time.Second),
		StaleAfterOverrides: p.overrides("TICKGUARD_STALE_AFTER_OVERRIDES"),
		DuplicateWindow:     p.integer("TICKGUARD_DUPLICATE_WINDOW", 10_000),
		SpikeWindow:         p.integer("TICKGUARD_SPIKE_WINDOW", 101),
		SpikeK:              p.float("TICKGUARD_SPIKE_K", 10),
		SpikeMinDeviation:   p.float("TICKGUARD_SPIKE_MIN_DEVIATION", 0.005),

		Buffer:   p.integer("TICKGUARD_BUFFER", 4096),
		BarGrace: p.duration("TICKGUARD_BAR_GRACE", 2*time.Second),
	}
	p.check(c.HTTPAddr != "", "TICKGUARD_HTTP_ADDR must not be empty")
	p.check(c.DataDir != "", "TICKGUARD_DATA_DIR must not be empty")
	p.check(c.DBPath != "", "TICKGUARD_DB_PATH must not be empty")
	p.check(slices.ContainsFunc(c.Feeds(), Feed.On),
		"at least one of TICKGUARD_BINANCE_SYMBOLS, TICKGUARD_KRAKEN_SYMBOLS and TICKGUARD_MERCADOBITCOIN_SYMBOLS must list a symbol, or TICKGUARD_KLEVER_NETWORK must be set")
	p.check(len(c.Klever.Symbols) <= 1, "TICKGUARD_KLEVER_NETWORK must name one network")
	p.check(c.ShutdownTimeout > 0, "TICKGUARD_SHUTDOWN_TIMEOUT must be positive")
	p.check(c.Buffer > 0, "TICKGUARD_BUFFER must be positive")
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		p.fail("TICKGUARD_LOG_LEVEL must be debug, info, warn or error, got %q", c.LogLevel)
	}
	if err := errors.Join(p.errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return c, nil
}

type parser struct {
	lookup Lookup
	errs   []error
}

func (p *parser) fail(format string, args ...any) {
	p.errs = append(p.errs, fmt.Errorf(format, args...))
}

func (p *parser) check(ok bool, msg string) {
	if !ok {
		p.errs = append(p.errs, errors.New(msg))
	}
}

func (p *parser) str(key, def string) string {
	if v, ok := p.lookup(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

// list splits a comma-separated value. Set but empty means "none", which is
// how one exchange is disabled; unset means the default.
func (p *parser) list(key, def string) []string {
	var out []string
	for _, s := range strings.Split(p.str(key, def), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// feed reads one source's URL and symbol list.
func (p *parser) feed(urlKey, urlDef, symbolsKey, symbolsDef string) Feed {
	return Feed{URL: p.str(urlKey, urlDef), Symbols: p.list(symbolsKey, symbolsDef)}
}

func (p *parser) duration(key string, def time.Duration) time.Duration {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil || d < 0 {
		p.fail("%s: want a non-negative duration such as 30s, got %q", key, v)
		return def
	}
	return d
}

func (p *parser) integer(key string, def int) int {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		p.fail("%s: want an integer, got %q", key, v)
		return def
	}
	return n
}

func (p *parser) float(key string, def float64) float64 {
	v, ok := p.lookup(key)
	if !ok {
		return def
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil {
		p.fail("%s: want a number, got %q", key, v)
		return def
	}
	return f
}

// overrides parses "binance:BTCUSDT=10s,kraken:ETH/USD=5m".
func (p *parser) overrides(key string) map[domain.StreamKey]time.Duration {
	out := map[domain.StreamKey]time.Duration{}
	v, ok := p.lookup(key)
	if !ok {
		return out
	}
	for _, item := range strings.Split(v, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		stream, dur, found := strings.Cut(item, "=")
		exchange, symbol, hasColon := strings.Cut(stream, ":")
		d, err := time.ParseDuration(dur)
		if !found || !hasColon || exchange == "" || symbol == "" || err != nil || d <= 0 {
			p.fail("%s: want exchange:SYMBOL=duration, got %q", key, item)
			continue
		}
		out[domain.StreamKey{Exchange: exchange, Symbol: symbol}] = d
	}
	return out
}

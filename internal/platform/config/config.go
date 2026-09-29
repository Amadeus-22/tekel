// Package config reads tickguard's configuration from environment variables,
// once, at startup. Every variable is documented in the README table.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the validated process configuration. It holds plain values only;
// cmd/tickguard maps them onto the packages that use them.
type Config struct {
	HTTPAddr        string
	LogLevel        string
	DataDir         string
	DBPath          string
	ShutdownTimeout time.Duration

	BinanceURL     string
	BinanceSymbols []string
	KrakenURL      string
	KrakenSymbols  []string
	// MercadoBitcoin is off unless symbols are set: LEAN needs a custom market
	// to read its output (docs/adr/0006).
	MercadoBitcoinURL     string
	MercadoBitcoinSymbols []string

	ClockSkew           time.Duration
	OutOfOrderTolerance time.Duration
	StaleAfter          time.Duration
	// StaleAfterOverrides maps "exchange:SYMBOL" to a per-stream stale threshold.
	StaleAfterOverrides map[string]time.Duration
	DuplicateWindow     int
	SpikeWindow         int
	SpikeK              float64
	SpikeMinDeviation   float64

	Buffer   int
	BarGrace time.Duration
}

// Lookup is os.LookupEnv's signature, injectable for tests.
type Lookup func(key string) (string, bool)

// FromEnv loads the configuration from the process environment.
func FromEnv() (Config, error) { return Load(os.LookupEnv) }

// Load reads every variable through lookup and reports all problems at once, so
// a misconfigured deployment fails with the full list rather than one at a time.
func Load(lookup Lookup) (Config, error) {
	p := parser{lookup: lookup}
	c := Config{
		HTTPAddr:        p.str("TICKGUARD_HTTP_ADDR", ":8080"),
		LogLevel:        p.str("TICKGUARD_LOG_LEVEL", "info"),
		DataDir:         p.str("TICKGUARD_DATA_DIR", "./data"),
		DBPath:          p.str("TICKGUARD_DB_PATH", "./tickguard.db"),
		ShutdownTimeout: p.duration("TICKGUARD_SHUTDOWN_TIMEOUT", 10*time.Second),

		BinanceURL:     p.str("TICKGUARD_BINANCE_URL", "wss://stream.binance.com:9443"),
		BinanceSymbols: p.list("TICKGUARD_BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT"),
		KrakenURL:      p.str("TICKGUARD_KRAKEN_URL", "wss://ws.kraken.com/v2"),
		KrakenSymbols:  p.list("TICKGUARD_KRAKEN_SYMBOLS", "BTC/USD,ETH/USD"),

		MercadoBitcoinURL:     p.str("TICKGUARD_MERCADOBITCOIN_URL", "wss://ws.mercadobitcoin.net/ws"),
		MercadoBitcoinSymbols: p.list("TICKGUARD_MERCADOBITCOIN_SYMBOLS", ""),

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
	p.check(len(c.BinanceSymbols)+len(c.KrakenSymbols)+len(c.MercadoBitcoinSymbols) > 0,
		"at least one of TICKGUARD_BINANCE_SYMBOLS, TICKGUARD_KRAKEN_SYMBOLS and TICKGUARD_MERCADOBITCOIN_SYMBOLS must list a symbol")
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
func (p *parser) overrides(key string) map[string]time.Duration {
	out := map[string]time.Duration{}
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
		out[exchange+":"+symbol] = d
	}
	return out
}

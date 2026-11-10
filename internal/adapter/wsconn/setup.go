package wsconn

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// CheckSymbols validates an adapter's configured symbols: at least one, each
// matching re. hint completes "symbol %q must ..." with the expected spelling,
// e.g. "look like BTC/USD". Errors are prefixed with name.
func CheckSymbols(name string, symbols []string, re *regexp.Regexp, hint string) error {
	if len(symbols) == 0 {
		return errors.New(name + ": at least one symbol is required")
	}
	for _, s := range symbols {
		if !re.MatchString(s) {
			return fmt.Errorf("%s: symbol %q must %s", name, s, hint)
		}
	}
	return nil
}

// CapReadTimeout sets ReadTimeout to limit when it is unset or longer. limit is
// the longest silence the exchange's own heartbeat allows.
func (c *Config) CapReadTimeout(limit time.Duration) {
	if c.ReadTimeout == 0 || c.ReadTimeout > limit {
		c.ReadTimeout = limit
	}
}

// Bind names the connection, points it at url and gives it the adapter's logger.
func (c *Config) Bind(name, url string, log *slog.Logger) {
	c.Name = name
	c.URL = url
	c.Logger = log
}

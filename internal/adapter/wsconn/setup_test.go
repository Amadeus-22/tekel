package wsconn

import (
	"log/slog"
	"regexp"
	"testing"
	"time"
)

func TestCheckSymbols(t *testing.T) {
	re := regexp.MustCompile(`^[A-Z]+/[A-Z]+$`)
	for _, tt := range []struct {
		symbols []string
		want    string
	}{
		{nil, "ex: at least one symbol is required"},
		{[]string{"BTC/USD", "btc/usd"}, `ex: symbol "btc/usd" must look like BTC/USD`},
		{[]string{"BTC/USD", "ETH/USD"}, ""},
	} {
		err := CheckSymbols("ex", tt.symbols, re, "look like BTC/USD")
		if got := errString(err); got != tt.want {
			t.Errorf("CheckSymbols(%v) = %q, want %q", tt.symbols, got, tt.want)
		}
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestCapReadTimeout(t *testing.T) {
	for _, tt := range []struct{ in, want time.Duration }{
		{0, 15 * time.Second},                // unset: the cap
		{time.Minute, 15 * time.Second},      // longer than the heartbeat allows
		{5 * time.Second, 5 * time.Second},   // shorter: kept
		{15 * time.Second, 15 * time.Second}, // equal: kept
	} {
		c := Config{ReadTimeout: tt.in}
		c.CapReadTimeout(15 * time.Second)
		if c.ReadTimeout != tt.want {
			t.Errorf("CapReadTimeout(%v) = %v, want %v", tt.in, c.ReadTimeout, tt.want)
		}
	}
}

func TestBind(t *testing.T) {
	log := slog.New(slog.DiscardHandler)
	c := DefaultConfig()
	c.Bind("ex", "wss://example/ws", log)
	if c.Name != "ex" || c.URL != "wss://example/ws" || c.Logger != log || c.ReadTimeout != DefaultConfig().ReadTimeout {
		t.Errorf("Bind = %+v", c)
	}
}

package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) Lookup {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestDefaults(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.LogLevel != "info" || c.DataDir != "./data" || c.DBPath != "./tickguard.db" {
		t.Errorf("basic defaults = %+v", c)
	}
	if strings.Join(c.BinanceSymbols, ",") != "BTCUSDT,ETHUSDT" || strings.Join(c.KrakenSymbols, ",") != "BTC/USD,ETH/USD" {
		t.Errorf("symbols = %v %v", c.BinanceSymbols, c.KrakenSymbols)
	}
	if c.ClockSkew != 2*time.Second || c.StaleAfter != time.Minute || c.SpikeK != 10 || c.Buffer != 4096 {
		t.Errorf("thresholds = %+v", c)
	}
	if len(c.StaleAfterOverrides) != 0 {
		t.Errorf("overrides = %v", c.StaleAfterOverrides)
	}
}

func TestOverridesFromEnv(t *testing.T) {
	c, err := Load(env(map[string]string{
		"TICKGUARD_HTTP_ADDR":             "127.0.0.1:9000",
		"TICKGUARD_LOG_LEVEL":             "debug",
		"TICKGUARD_BINANCE_SYMBOLS":       " BTCUSDT , ,SOLUSDT",
		"TICKGUARD_KRAKEN_SYMBOLS":        "", // disables Kraken
		"TICKGUARD_CLOCK_SKEW":            "500ms",
		"TICKGUARD_DUPLICATE_WINDOW":      "50",
		"TICKGUARD_SPIKE_MIN_DEVIATION":   "0.01",
		"TICKGUARD_STALE_AFTER_OVERRIDES": "binance:SOLUSDT=5m, kraken:ETH/USD=90s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:9000" || c.LogLevel != "debug" || c.ClockSkew != 500*time.Millisecond ||
		c.DuplicateWindow != 50 || c.SpikeMinDeviation != 0.01 {
		t.Errorf("config = %+v", c)
	}
	if strings.Join(c.BinanceSymbols, ",") != "BTCUSDT,SOLUSDT" || len(c.KrakenSymbols) != 0 {
		t.Errorf("symbols = %q %q", c.BinanceSymbols, c.KrakenSymbols)
	}
	if c.StaleAfterOverrides["binance:SOLUSDT"] != 5*time.Minute || c.StaleAfterOverrides["kraken:ETH/USD"] != 90*time.Second {
		t.Errorf("overrides = %v", c.StaleAfterOverrides)
	}
}

func TestReportsEveryError(t *testing.T) {
	_, err := Load(env(map[string]string{
		"TICKGUARD_HTTP_ADDR":             "",
		"TICKGUARD_DATA_DIR":              "",
		"TICKGUARD_DB_PATH":               "",
		"TICKGUARD_LOG_LEVEL":             "loud",
		"TICKGUARD_CLOCK_SKEW":            "2",
		"TICKGUARD_BAR_GRACE":             "-1s",
		"TICKGUARD_DUPLICATE_WINDOW":      "many",
		"TICKGUARD_SPIKE_K":               "ten",
		"TICKGUARD_BUFFER":                "0",
		"TICKGUARD_SHUTDOWN_TIMEOUT":      "0s",
		"TICKGUARD_STALE_AFTER_OVERRIDES": "BTCUSDT=5m,binance:ETHUSDT=soon,kraken:=1s",
		"TICKGUARD_BINANCE_SYMBOLS":       "",
		"TICKGUARD_KRAKEN_SYMBOLS":        " , ",
	}))
	if err == nil {
		t.Fatal("Load() = nil error")
	}
	for _, want := range []string{
		"TICKGUARD_HTTP_ADDR", "TICKGUARD_DATA_DIR", "TICKGUARD_DB_PATH", "TICKGUARD_LOG_LEVEL",
		"TICKGUARD_CLOCK_SKEW", "TICKGUARD_BAR_GRACE", "TICKGUARD_DUPLICATE_WINDOW", "TICKGUARD_SPIKE_K",
		"TICKGUARD_BUFFER", "TICKGUARD_SHUTDOWN_TIMEOUT", `"BTCUSDT=5m"`, `"binance:ETHUSDT=soon"`,
		`"kraken:=1s"`, "at least one of",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("TICKGUARD_HTTP_ADDR", ":9999")
	c, err := FromEnv()
	if err != nil || c.HTTPAddr != ":9999" {
		t.Errorf("FromEnv() = %+v, %v", c.HTTPAddr, err)
	}
}

func TestMercadoBitcoin(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.MercadoBitcoinSymbols) != 0 || c.MercadoBitcoinURL != "wss://ws.mercadobitcoin.net/ws" {
		t.Errorf("default = %v %q, want disabled", c.MercadoBitcoinSymbols, c.MercadoBitcoinURL)
	}
	// Mercado Bitcoin alone is a valid configuration.
	c, err = Load(env(map[string]string{
		"TICKGUARD_BINANCE_SYMBOLS": "", "TICKGUARD_KRAKEN_SYMBOLS": "",
		"TICKGUARD_MERCADOBITCOIN_SYMBOLS": "BTC-BRL, USDT-BRL",
	}))
	if err != nil || strings.Join(c.MercadoBitcoinSymbols, ",") != "BTC-BRL,USDT-BRL" {
		t.Errorf("symbols = %v, err %v", c.MercadoBitcoinSymbols, err)
	}
	_, err = Load(env(map[string]string{"TICKGUARD_BINANCE_SYMBOLS": "", "TICKGUARD_KRAKEN_SYMBOLS": ""}))
	if err == nil || !strings.Contains(err.Error(), "TICKGUARD_MERCADOBITCOIN_SYMBOLS") {
		t.Errorf("no symbols at all: err = %v", err)
	}
}

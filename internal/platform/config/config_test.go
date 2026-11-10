package config

import (
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
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
	if c.HTTPAddr != "127.0.0.1:8080" || c.LogLevel != "info" || c.DataDir != "./data" || c.DBPath != "./tekel.db" {
		t.Errorf("basic defaults = %+v", c)
	}
	if strings.Join(c.Binance.Symbols, ",") != "BTCUSDT,ETHUSDT" || strings.Join(c.Kraken.Symbols, ",") != "BTC/USD,ETH/USD" {
		t.Errorf("symbols = %v %v", c.Binance.Symbols, c.Kraken.Symbols)
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
		"TEKEL_HTTP_ADDR":             "127.0.0.1:9000",
		"TEKEL_LOG_LEVEL":             "debug",
		"TEKEL_BINANCE_SYMBOLS":       " BTCUSDT , ,SOLUSDT",
		"TEKEL_KRAKEN_SYMBOLS":        "", // disables Kraken
		"TEKEL_CLOCK_SKEW":            "500ms",
		"TEKEL_DUPLICATE_WINDOW":      "50",
		"TEKEL_SPIKE_MIN_DEVIATION":   "0.01",
		"TEKEL_STALE_AFTER_OVERRIDES": "binance:SOLUSDT=5m, kraken:ETH/USD=90s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:9000" || c.LogLevel != "debug" || c.ClockSkew != 500*time.Millisecond ||
		c.DuplicateWindow != 50 || c.SpikeMinDeviation != 0.01 {
		t.Errorf("config = %+v", c)
	}
	if strings.Join(c.Binance.Symbols, ",") != "BTCUSDT,SOLUSDT" || len(c.Kraken.Symbols) != 0 {
		t.Errorf("symbols = %q %q", c.Binance.Symbols, c.Kraken.Symbols)
	}
	sol := domain.StreamKey{Exchange: "binance", Symbol: "SOLUSDT"}
	eth := domain.StreamKey{Exchange: "kraken", Symbol: "ETH/USD"}
	if c.StaleAfterOverrides[sol] != 5*time.Minute || c.StaleAfterOverrides[eth] != 90*time.Second {
		t.Errorf("overrides = %v", c.StaleAfterOverrides)
	}
}

func TestReportsEveryError(t *testing.T) {
	_, err := Load(env(map[string]string{
		"TEKEL_HTTP_ADDR":             "",
		"TEKEL_DATA_DIR":              "",
		"TEKEL_DB_PATH":               "",
		"TEKEL_LOG_LEVEL":             "loud",
		"TEKEL_CLOCK_SKEW":            "2",
		"TEKEL_BAR_GRACE":             "-1s",
		"TEKEL_DUPLICATE_WINDOW":      "many",
		"TEKEL_SPIKE_K":               "ten",
		"TEKEL_BUFFER":                "0",
		"TEKEL_SHUTDOWN_TIMEOUT":      "0s",
		"TEKEL_STALE_AFTER_OVERRIDES": "BTCUSDT=5m,binance:ETHUSDT=soon,kraken:=1s",
		"TEKEL_BINANCE_SYMBOLS":       "",
		"TEKEL_KRAKEN_SYMBOLS":        " , ",
	}))
	if err == nil {
		t.Fatal("Load() = nil error")
	}
	for _, want := range []string{
		"TEKEL_HTTP_ADDR", "TEKEL_DATA_DIR", "TEKEL_DB_PATH", "TEKEL_LOG_LEVEL",
		"TEKEL_CLOCK_SKEW", "TEKEL_BAR_GRACE", "TEKEL_DUPLICATE_WINDOW", "TEKEL_SPIKE_K",
		"TEKEL_BUFFER", "TEKEL_SHUTDOWN_TIMEOUT", `"BTCUSDT=5m"`, `"binance:ETHUSDT=soon"`,
		`"kraken:=1s"`, "at least one of",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error does not mention %s:\n%v", want, err)
		}
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv("TEKEL_HTTP_ADDR", ":9999")
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
	if len(c.MercadoBitcoin.Symbols) != 0 || c.MercadoBitcoin.URL != "wss://ws.mercadobitcoin.net/ws" {
		t.Errorf("default = %v %q, want disabled", c.MercadoBitcoin.Symbols, c.MercadoBitcoin.URL)
	}
	// Mercado Bitcoin alone is a valid configuration.
	c, err = Load(env(map[string]string{
		"TEKEL_BINANCE_SYMBOLS": "", "TEKEL_KRAKEN_SYMBOLS": "",
		"TEKEL_MERCADOBITCOIN_SYMBOLS": "BTC-BRL, USDT-BRL",
	}))
	if err != nil || strings.Join(c.MercadoBitcoin.Symbols, ",") != "BTC-BRL,USDT-BRL" {
		t.Errorf("symbols = %v, err %v", c.MercadoBitcoin.Symbols, err)
	}
	_, err = Load(env(map[string]string{"TEKEL_BINANCE_SYMBOLS": "", "TEKEL_KRAKEN_SYMBOLS": ""}))
	if err == nil || !strings.Contains(err.Error(), "TEKEL_MERCADOBITCOIN_SYMBOLS") {
		t.Errorf("no symbols at all: err = %v", err)
	}
}

func TestKlever(t *testing.T) {
	c, err := Load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if c.Klever.On() || c.Klever.URL != "" {
		t.Errorf("default = %v %q, want disabled", c.Klever.Symbols, c.Klever.URL)
	}
	// The block feed alone is a valid configuration.
	c, err = Load(env(map[string]string{
		"TEKEL_BINANCE_SYMBOLS": "", "TEKEL_KRAKEN_SYMBOLS": "",
		"TEKEL_KLEVER_NETWORK": " mainnet ", "TEKEL_KLEVER_URL": "wss://node.example/subscribe",
	}))
	if err != nil || strings.Join(c.Klever.Symbols, ",") != "mainnet" || c.Klever.URL != "wss://node.example/subscribe" {
		t.Errorf("klever = %v %q, err %v", c.Klever.Symbols, c.Klever.URL, err)
	}
	_, err = Load(env(map[string]string{"TEKEL_KLEVER_NETWORK": "mainnet,testnet"}))
	if err == nil || !strings.Contains(err.Error(), "TEKEL_KLEVER_NETWORK must name one network") {
		t.Errorf("two networks: err = %v", err)
	}
	_, err = Load(env(map[string]string{"TEKEL_BINANCE_SYMBOLS": "", "TEKEL_KRAKEN_SYMBOLS": ""}))
	if err == nil || !strings.Contains(err.Error(), "TEKEL_KLEVER_NETWORK") {
		t.Errorf("no feed at all: err = %v", err)
	}
}

func TestFeedsOrder(t *testing.T) {
	c, err := Load(env(map[string]string{
		"TEKEL_MERCADOBITCOIN_SYMBOLS": "BTC-BRL", "TEKEL_KLEVER_NETWORK": "testnet",
	}))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range c.Feeds() {
		got = append(got, f.Symbols[0])
	}
	if want := "BTCUSDT BTC/USD BTC-BRL testnet"; strings.Join(got, " ") != want {
		t.Errorf("Feeds() first symbols = %v, want %s", got, want)
	}
}

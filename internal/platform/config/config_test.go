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

// sources mirrors the shape of cmd/tekel's table: one source on by default,
// one off by default, one single-valued.
var sources = []Source{
	{Name: "alpha", URLKey: "TEKEL_ALPHA_URL", URLDefault: "wss://alpha", SymbolsKey: "TEKEL_ALPHA_SYMBOLS", SymbolsDefault: "A1,A2"},
	{Name: "beta", URLKey: "TEKEL_BETA_URL", URLDefault: "wss://beta", SymbolsKey: "TEKEL_BETA_SYMBOLS"},
	{Name: "chain", URLKey: "TEKEL_CHAIN_URL", SymbolsKey: "TEKEL_CHAIN_NETWORK", Single: true},
}

func TestDefaults(t *testing.T) {
	c, err := Load(env(nil), sources)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:8080" || c.LogLevel != "info" || c.DataDir != "./data" || c.DBPath != "./tekel.db" {
		t.Errorf("basic defaults = %+v", c)
	}
	if c.ClockSkew != 2*time.Second || c.StaleAfter != time.Minute || c.SpikeK != 10 || c.Buffer != 4096 {
		t.Errorf("thresholds = %+v", c)
	}
	if len(c.StaleAfterOverrides) != 0 || len(c.ClockSkewOverrides) != 0 {
		t.Errorf("overrides = %v", c.StaleAfterOverrides)
	}
}

func TestOverridesFromEnv(t *testing.T) {
	c, err := Load(env(map[string]string{
		"TEKEL_HTTP_ADDR":             "127.0.0.1:9000",
		"TEKEL_LOG_LEVEL":             "debug",
		"TEKEL_CLOCK_SKEW":            "500ms",
		"TEKEL_DUPLICATE_WINDOW":      "50",
		"TEKEL_SPIKE_MIN_DEVIATION":   "0.01",
		"TEKEL_STALE_AFTER_OVERRIDES": "binance:SOLUSDT=5m, kraken:ETH/USD=90s",
		"TEKEL_CLOCK_SKEW_OVERRIDES":  "klever:mainnet=5s",
	}), sources)
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != "127.0.0.1:9000" || c.LogLevel != "debug" || c.ClockSkew != 500*time.Millisecond ||
		c.DuplicateWindow != 50 || c.SpikeMinDeviation != 0.01 {
		t.Errorf("config = %+v", c)
	}
	sol := domain.StreamKey{Exchange: "binance", Symbol: "SOLUSDT"}
	eth := domain.StreamKey{Exchange: "kraken", Symbol: "ETH/USD"}
	if c.StaleAfterOverrides[sol] != 5*time.Minute || c.StaleAfterOverrides[eth] != 90*time.Second {
		t.Errorf("overrides = %v", c.StaleAfterOverrides)
	}
	if c.ClockSkewOverrides[domain.StreamKey{Exchange: "klever", Symbol: "mainnet"}] != 5*time.Second {
		t.Errorf("clock skew overrides = %v", c.ClockSkewOverrides)
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
		"TEKEL_ALPHA_SYMBOLS":         " , ",
	}), sources)
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
	c, err := FromEnv(sources)
	if err != nil || c.HTTPAddr != ":9999" {
		t.Errorf("FromEnv() = %+v, %v", c.HTTPAddr, err)
	}
}

func TestFeeds(t *testing.T) {
	c, err := Load(env(nil), sources)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range c.Feeds {
		got = append(got, f.Name+"|"+f.URL+"|"+strings.Join(f.Symbols, ","))
	}
	if want := "alpha|wss://alpha|A1,A2 beta|wss://beta| chain||"; strings.Join(got, " ") != want {
		t.Errorf("default feeds = %v, want %s (declared order, defaults applied)", got, want)
	}
	if c.Feed("beta").On() || !c.Feed("alpha").On() || c.Feed("nope") != nil {
		t.Errorf("On/Feed lookup wrong: %+v", c.Feeds)
	}

	c, err = Load(env(map[string]string{
		"TEKEL_ALPHA_SYMBOLS": "", // turns alpha off
		"TEKEL_BETA_SYMBOLS":  " B1 , ,B2",
		"TEKEL_CHAIN_NETWORK": " mainnet ", "TEKEL_CHAIN_URL": "wss://node",
	}), sources)
	if err != nil {
		t.Fatal(err)
	}
	if c.Feed("alpha").On() || strings.Join(c.Feed("beta").Symbols, ",") != "B1,B2" ||
		strings.Join(c.Feed("chain").Symbols, ",") != "mainnet" || c.Feed("chain").URL != "wss://node" {
		t.Errorf("overridden feeds = %+v", c.Feeds)
	}
}

func TestFeedsRejected(t *testing.T) {
	_, err := Load(env(map[string]string{"TEKEL_CHAIN_NETWORK": "mainnet,testnet"}), sources)
	if err == nil || !strings.Contains(err.Error(), "TEKEL_CHAIN_NETWORK must name one value") {
		t.Errorf("two values for a single source: err = %v", err)
	}
	_, err = Load(env(map[string]string{"TEKEL_ALPHA_SYMBOLS": ""}), sources)
	if err == nil || !strings.Contains(err.Error(), "TEKEL_ALPHA_SYMBOLS, TEKEL_BETA_SYMBOLS, TEKEL_CHAIN_NETWORK") {
		t.Errorf("no feed on: err = %v, want every symbols variable named", err)
	}
}

func TestAlerts(t *testing.T) {
	c, err := Load(env(nil), sources)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.AlertKinds) != 2 || c.AlertKinds[0] != domain.KindSequenceGap || c.WebhookURL != "" || c.TelegramBotToken != "" {
		t.Errorf("alert defaults = %v %q %q, want gaps and stale streams, no sender", c.AlertKinds, c.WebhookURL, c.TelegramBotToken)
	}
	_, err = Load(env(map[string]string{
		"TEKEL_ALERT_KINDS":        "sequence_gap,nope",
		"TEKEL_TELEGRAM_BOT_TOKEN": "t",
		"TEKEL_WEBHOOK_URL":        "ftp://x",
	}), sources)
	for _, want := range []string{"TEKEL_ALERT_KINDS", "must be set together", "TEKEL_WEBHOOK_URL must be an http(s) URL"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %q", err, want)
		}
	}
}

func TestReference(t *testing.T) {
	c, err := Load(env(nil), sources)
	if err != nil {
		t.Fatal(err)
	}
	if c.CMCAPIKey != "" || c.ReferenceInterval != time.Minute || c.ReferenceThreshold != 0.01 ||
		c.CMCURL != "https://pro-api.coinmarketcap.com" {
		t.Errorf("reference defaults = %q %v %v %q, want off, 1m, 1%%", c.CMCAPIKey, c.ReferenceInterval, c.ReferenceThreshold, c.CMCURL)
	}
	_, err = Load(env(map[string]string{
		"TEKEL_REFERENCE_INTERVAL":  "5s",
		"TEKEL_REFERENCE_THRESHOLD": "2",
	}), sources)
	for _, want := range []string{"TEKEL_REFERENCE_INTERVAL must be at least 10s", "TEKEL_REFERENCE_THRESHOLD must be between 0 and 1"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %q", err, want)
		}
	}
}

package main

import (
	"context"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/platform/config"
	"github.com/Amadeus-22/tekel/internal/platform/metrics"
)

func TestPipelineConfigMapsEveryField(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		v, ok := map[string]string{
			"TEKEL_SPIKE_WINDOW":          "11",
			"TEKEL_STALE_AFTER_OVERRIDES": "kraken:ETH/USD=5m",
			"TEKEL_CLOCK_SKEW_OVERRIDES":  "kraken:ETH/USD=4s",
			"TEKEL_BUFFER":                "8",
		}[k]
		return v, ok
	}, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	pc := pipelineConfig(cfg)
	if err := pc.Quality.Validate(); err != nil {
		t.Fatalf("mapped quality config invalid: %v", err)
	}
	if pc.Quality.SpikeWindow != 11 || pc.Quality.SpikeMinSamples != 11 || pc.Buffer != 8 {
		t.Errorf("pipeline config = %+v", pc)
	}
	if pc.StaleAfter[domain.StreamKey{Exchange: "kraken", Symbol: "ETH/USD"}] != 5*time.Minute {
		t.Errorf("stale overrides = %v", pc.StaleAfter)
	}
	if pc.ClockSkew[domain.StreamKey{Exchange: "kraken", Symbol: "ETH/USD"}] != 4*time.Second {
		t.Errorf("clock skew overrides = %v", pc.ClockSkew)
	}
}

func TestBuildFeedsHonoursDisabledExchange(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		if k == "TEKEL_KRAKEN_SYMBOLS" || k == "TEKEL_COINBASE_SYMBOLS" {
			return "", true
		}
		return "", false
	}, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	feeds, err := buildFeeds(cfg, slog.New(slog.DiscardHandler), metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 1 || feeds[0].Exchange() != "binance" {
		t.Errorf("feeds = %v", feeds)
	}

	cfg.Feed("mercadobitcoin").Symbols = []string{"BTC-BRL"}
	feeds, err = buildFeeds(cfg, slog.Default(), metrics.New())
	if err != nil || len(feeds) != 2 || feeds[1].Exchange() != "mercadobitcoin" {
		t.Errorf("feeds with Mercado Bitcoin = %v, %v", feeds, err)
	}
	cfg.Feed("mercadobitcoin").Symbols = []string{"BRLBTC"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("Mercado Bitcoin symbol in WebSocket spelling accepted")
	}
	cfg.Feed("mercadobitcoin").Symbols = nil

	cfg.Feed("klever").Symbols = []string{"testnet"}
	feeds, err = buildFeeds(cfg, slog.Default(), metrics.New())
	if err != nil || len(feeds) != 2 || feeds[1].Exchange() != "klever" || feeds[1].Kind() != domain.StreamBlocks {
		t.Errorf("feeds with Klever = %v, %v", feeds, err)
	}
	cfg.Feed("klever").Symbols = []string{"mainnet"} // no URL: the public mainnet node
	if feeds, err = buildFeeds(cfg, slog.Default(), metrics.New()); err != nil || len(feeds) != 2 || feeds[1].Symbols()[0] != "mainnet" {
		t.Errorf("feeds with Klever mainnet = %v, %v", feeds, err)
	}
	cfg.Feed("klever").Symbols = nil

	cfg.Feed("binance").Symbols = []string{"not valid"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("invalid symbol accepted")
	}
}

func TestBuildFeedsOrder(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		v, ok := map[string]string{
			"TEKEL_MERCADOBITCOIN_SYMBOLS": "BTC-BRL",
			"TEKEL_KLEVER_NETWORK":         "testnet",
		}[k]
		return v, ok
	}, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	feeds, err := buildFeeds(cfg, slog.New(slog.DiscardHandler), metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range feeds {
		got = append(got, f.Exchange())
	}
	if want := "binance kraken coinbase mercadobitcoin klever"; strings.Join(got, " ") != want {
		t.Errorf("feeds = %v, want %s", got, want)
	}
}

// TestConfigDefaultsMatchPackageDefaults guards against drift: an unset
// environment must give exactly the defaults the app package declares. Feed
// URL defaults cannot drift: sources.go reads them from the adapters.
func TestConfigDefaultsMatchPackageDefaults(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false }, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	pc := pipelineConfig(cfg)
	if len(pc.StaleAfter) != 0 {
		t.Errorf("default stale overrides = %v", pc.StaleAfter)
	}
	if len(pc.ClockSkew) != 0 {
		t.Errorf("default clock skew overrides = %v", pc.ClockSkew)
	}
	pc.StaleAfter, pc.ClockSkew = nil, nil
	if want := app.DefaultConfig(); !reflect.DeepEqual(pc, want) {
		t.Errorf("pipeline config from defaults =\n%+v\nwant app.DefaultConfig() =\n%+v", pc, want)
	}
}

func TestSourcesTable(t *testing.T) {
	names, keys := map[string]bool{}, map[string]bool{}
	for _, s := range sources {
		if s.Name == "" || s.SymbolsKey == "" || s.URLKey == "" || s.build == nil {
			t.Errorf("incomplete source %+v", s.Source)
		}
		if names[s.Name] || keys[s.SymbolsKey] || keys[s.URLKey] {
			t.Errorf("source %q repeats a name or an environment variable", s.Name)
		}
		names[s.Name], keys[s.SymbolsKey], keys[s.URLKey] = true, true, true
	}
	cfg, err := config.Load(func(string) (string, bool) { return "", false }, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	var on []string
	for _, f := range cfg.Feeds {
		if f.On() {
			on = append(on, f.Name+"="+strings.Join(f.Symbols, ","))
		}
	}
	if want := "binance=BTCUSDT,ETHUSDT kraken=BTC/USD,ETH/USD coinbase=BTC-USD,ETH-USD"; strings.Join(on, " ") != want {
		t.Errorf("feeds on by default = %v, want %s", on, want)
	}
}

func TestAlertingIsOffWithoutASender(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false }, sourceConfigs())
	if err != nil {
		t.Fatal(err)
	}
	store := &nopStore{}
	got, alerts, err := alerting(cfg, store, slog.New(slog.DiscardHandler), metrics.New())
	if err != nil || alerts != nil || got != store {
		t.Fatalf("alerting with no sender = %v, %v, %v; want the plain store and no notifier", got, alerts, err)
	}
	cfg.TelegramBotToken, cfg.TelegramChatID = "t", "c"
	if got, alerts, err = alerting(cfg, store, slog.New(slog.DiscardHandler), metrics.New()); err != nil || alerts == nil || got == store {
		t.Fatalf("alerting with Telegram = %v, %v, %v; want a notifier wrapping the store", got, alerts, err)
	}
}

type nopStore struct{}

func (*nopStore) Save(context.Context, domain.Incident) error { return nil }

package main

import (
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/adapter/binance"
	"github.com/Amadeus-22/tickguard/internal/adapter/kraken"
	"github.com/Amadeus-22/tickguard/internal/adapter/mercadobitcoin"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
	"github.com/Amadeus-22/tickguard/internal/platform/config"
	"github.com/Amadeus-22/tickguard/internal/platform/metrics"
)

func TestPipelineConfigMapsEveryField(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		v, ok := map[string]string{
			"TICKGUARD_SPIKE_WINDOW":          "11",
			"TICKGUARD_STALE_AFTER_OVERRIDES": "kraken:ETH/USD=5m",
			"TICKGUARD_BUFFER":                "8",
		}[k]
		return v, ok
	})
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
}

func TestBuildFeedsHonoursDisabledExchange(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		if k == "TICKGUARD_KRAKEN_SYMBOLS" {
			return "", true
		}
		return "", false
	})
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

	cfg.MercadoBitcoin.Symbols = []string{"BTC-BRL"}
	feeds, err = buildFeeds(cfg, slog.Default(), metrics.New())
	if err != nil || len(feeds) != 2 || feeds[1].Exchange() != "mercadobitcoin" {
		t.Errorf("feeds with Mercado Bitcoin = %v, %v", feeds, err)
	}
	cfg.MercadoBitcoin.Symbols = []string{"BRLBTC"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("Mercado Bitcoin symbol in WebSocket spelling accepted")
	}
	cfg.MercadoBitcoin.Symbols = nil

	cfg.Klever.Symbols = []string{"testnet"}
	feeds, err = buildFeeds(cfg, slog.Default(), metrics.New())
	if err != nil || len(feeds) != 2 || feeds[1].Exchange() != "klever" || feeds[1].Kind() != domain.StreamBlocks {
		t.Errorf("feeds with Klever = %v, %v", feeds, err)
	}
	cfg.Klever.Symbols = []string{"mainnet"} // no public mainnet node serves /subscribe: a URL is required
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("Klever mainnet without a URL accepted")
	}
	cfg.Klever.Symbols = nil

	cfg.Binance.Symbols = []string{"not valid"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("invalid symbol accepted")
	}
}

func TestBuildFeedsOrder(t *testing.T) {
	cfg, err := config.Load(func(k string) (string, bool) {
		v, ok := map[string]string{
			"TICKGUARD_MERCADOBITCOIN_SYMBOLS": "BTC-BRL",
			"TICKGUARD_KLEVER_NETWORK":         "testnet",
		}[k]
		return v, ok
	})
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
	if want := "binance kraken mercadobitcoin klever"; strings.Join(got, " ") != want {
		t.Errorf("feeds = %v, want %s", got, want)
	}
}

// TestConfigDefaultsMatchPackageDefaults guards against drift: an unset
// environment must give exactly the defaults the packages themselves declare.
func TestConfigDefaultsMatchPackageDefaults(t *testing.T) {
	cfg, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	pc := pipelineConfig(cfg)
	if len(pc.StaleAfter) != 0 {
		t.Errorf("default stale overrides = %v", pc.StaleAfter)
	}
	pc.StaleAfter = nil
	if want := app.DefaultConfig(); !reflect.DeepEqual(pc, want) {
		t.Errorf("pipeline config from defaults =\n%+v\nwant app.DefaultConfig() =\n%+v", pc, want)
	}
	for _, tt := range []struct{ name, got, want string }{
		{"binance", cfg.Binance.URL, binance.DefaultBaseURL},
		{"kraken", cfg.Kraken.URL, kraken.DefaultURL},
		{"mercadobitcoin", cfg.MercadoBitcoin.URL, mercadobitcoin.DefaultURL},
		{"klever", cfg.Klever.URL, ""}, // empty: klever.New picks klever.TestnetURL
	} {
		if tt.got != tt.want {
			t.Errorf("%s default URL = %q, want %q", tt.name, tt.got, tt.want)
		}
	}
}

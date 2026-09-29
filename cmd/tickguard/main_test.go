package main

import (
	"io"
	"log/slog"
	"testing"
	"time"

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
	feeds, err := buildFeeds(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics.New())
	if err != nil {
		t.Fatal(err)
	}
	if len(feeds) != 1 || feeds[0].Exchange() != "binance" {
		t.Errorf("feeds = %v", feeds)
	}

	cfg.MercadoBitcoinSymbols = []string{"BTC-BRL"}
	feeds, err = buildFeeds(cfg, slog.Default(), metrics.New())
	if err != nil || len(feeds) != 2 || feeds[1].Exchange() != "mercadobitcoin" {
		t.Errorf("feeds with Mercado Bitcoin = %v, %v", feeds, err)
	}
	cfg.MercadoBitcoinSymbols = []string{"BRLBTC"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("Mercado Bitcoin symbol in WebSocket spelling accepted")
	}
	cfg.MercadoBitcoinSymbols = nil

	cfg.BinanceSymbols = []string{"not valid"}
	if _, err := buildFeeds(cfg, slog.Default(), metrics.New()); err == nil {
		t.Error("invalid symbol accepted")
	}
}

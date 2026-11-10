// Command tickguard collects public crypto trade streams, validates them as they
// arrive, records every data-quality incident and writes LEAN minute bars.
//
// "tickguard audit <path>" instead audits LEAN data already on disk; see audit.go.
//
// This file is wiring only: configuration, dependencies, start and shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/Amadeus-22/tickguard/internal/adapter/binance"
	"github.com/Amadeus-22/tickguard/internal/adapter/httpapi"
	"github.com/Amadeus-22/tickguard/internal/adapter/klever"
	"github.com/Amadeus-22/tickguard/internal/adapter/kraken"
	"github.com/Amadeus-22/tickguard/internal/adapter/leanfs"
	"github.com/Amadeus-22/tickguard/internal/adapter/mercadobitcoin"
	"github.com/Amadeus-22/tickguard/internal/adapter/sqlite"
	"github.com/Amadeus-22/tickguard/internal/adapter/wsconn"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/platform/config"
	"github.com/Amadeus-22/tickguard/internal/platform/logging"
	"github.com/Amadeus-22/tickguard/internal/platform/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "audit" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		code := runAudit(ctx, os.Args[2:], os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tickguard:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	log, err := logging.New(os.Stdout, cfg.LogLevel)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	store, err := sqlite.Open(ctx, cfg.DBPath)
	if err != nil {
		return err
	}
	defer store.Close()
	bars, err := leanfs.New(cfg.DataDir)
	if err != nil {
		return err
	}
	feeds, err := buildFeeds(cfg, log, m)
	if err != nil {
		return err
	}
	pipeline, err := app.NewPipeline(pipelineConfig(cfg), app.Deps{
		Feeds: feeds, Incidents: store, Bars: bars, Metrics: m, Logger: log,
	})
	if err != nil {
		return err
	}
	srv := httpapi.NewServer(cfg.HTTPAddr, httpapi.NewHandler(httpapi.Deps{
		Streams: pipeline, Incidents: store, Ready: store, Metrics: m.Handler(), Logger: log,
	}))

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return pipeline.Run(gctx) })
	g.Go(func() error {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	})

	log.Info("tickguard started", "http_addr", cfg.HTTPAddr, "data_dir", cfg.DataDir, "db_path", cfg.DBPath,
		"binance_symbols", strings.Join(cfg.Binance.Symbols, ","), "kraken_symbols", strings.Join(cfg.Kraken.Symbols, ","),
		"mercadobitcoin_symbols", strings.Join(cfg.MercadoBitcoin.Symbols, ","), "klever_network", strings.Join(cfg.Klever.Symbols, ","))
	err = g.Wait()
	log.Info("tickguard stopped", "error", fmt.Sprint(err))
	return err
}

// buildFeeds returns the configured feeds, in the order of this table. Adding
// an exchange is one row here plus its config.Feed.
func buildFeeds(cfg config.Config, log *slog.Logger, m *metrics.Metrics) ([]app.Feed, error) {
	type newFeed func(conn wsconn.Config, decodeErrors func()) (app.Feed, error)
	table := []struct {
		name  string
		cfg   config.Feed
		build newFeed
	}{
		{binance.Exchange, cfg.Binance, func(conn wsconn.Config, decodeErrors func()) (app.Feed, error) {
			return binance.New(binance.Config{BaseURL: cfg.Binance.URL, Symbols: cfg.Binance.Symbols,
				Conn: conn, Logger: log, DecodeErrors: decodeErrors})
		}},
		{kraken.Exchange, cfg.Kraken, func(conn wsconn.Config, decodeErrors func()) (app.Feed, error) {
			return kraken.New(kraken.Config{URL: cfg.Kraken.URL, Symbols: cfg.Kraken.Symbols,
				Conn: conn, Logger: log, DecodeErrors: decodeErrors})
		}},
		{mercadobitcoin.Exchange, cfg.MercadoBitcoin, func(conn wsconn.Config, decodeErrors func()) (app.Feed, error) {
			return mercadobitcoin.New(mercadobitcoin.Config{URL: cfg.MercadoBitcoin.URL, Symbols: cfg.MercadoBitcoin.Symbols,
				Conn: conn, Logger: log, DecodeErrors: decodeErrors})
		}},
		{klever.Chain, cfg.Klever, func(conn wsconn.Config, decodeErrors func()) (app.Feed, error) {
			return klever.New(klever.Config{URL: cfg.Klever.URL, Network: cfg.Klever.Symbols[0],
				Conn: conn, Logger: log, DecodeErrors: decodeErrors})
		}},
	}
	var feeds []app.Feed
	for _, row := range table {
		if !row.cfg.On() {
			continue
		}
		f, err := row.build(wsconn.DefaultConfig(), func() { m.DecodeError(row.name) })
		if err != nil {
			return nil, err
		}
		feeds = append(feeds, f)
	}
	return feeds, nil
}

func pipelineConfig(cfg config.Config) app.Config {
	pc := app.DefaultConfig()
	q := &pc.Quality
	q.ClockSkew = cfg.ClockSkew
	q.OutOfOrderTolerance = cfg.OutOfOrderTolerance
	q.StaleAfter = cfg.StaleAfter
	q.DuplicateWindow = cfg.DuplicateWindow
	q.SpikeWindow = cfg.SpikeWindow
	q.SpikeMinSamples = min(q.SpikeMinSamples, cfg.SpikeWindow)
	q.SpikeK = cfg.SpikeK
	q.SpikeMinDeviation = cfg.SpikeMinDeviation
	pc.Buffer = cfg.Buffer
	pc.BarGrace = cfg.BarGrace
	pc.StaleAfter = cfg.StaleAfterOverrides
	return pc
}

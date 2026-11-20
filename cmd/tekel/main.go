// Command tekel collects public crypto trade streams, validates them as they
// arrive, records every data-quality incident and writes LEAN minute bars.
//
// "tekel audit <path>" instead audits LEAN data already on disk; see audit.go.
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

	"github.com/Amadeus-22/tekel/internal/adapter/httpapi"
	"github.com/Amadeus-22/tekel/internal/adapter/leanfs"
	"github.com/Amadeus-22/tekel/internal/adapter/sqlite"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/platform/config"
	"github.com/Amadeus-22/tekel/internal/platform/logging"
	"github.com/Amadeus-22/tekel/internal/platform/metrics"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "audit" {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		code := runAudit(ctx, os.Args[2:], os.Stdout, os.Stderr)
		stop()
		os.Exit(code)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "tekel:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv(sourceConfigs())
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

	attrs := []any{"http_addr", cfg.HTTPAddr, "data_dir", cfg.DataDir, "db_path", cfg.DBPath}
	for _, f := range cfg.Feeds {
		attrs = append(attrs, f.Name+"_symbols", strings.Join(f.Symbols, ","))
	}
	log.Info("tekel started", attrs...)
	err = g.Wait()
	log.Info("tekel stopped", "error", fmt.Sprint(err))
	return err
}

// buildFeeds returns the adapters of the feeds that are on, in the order of
// the sources table (sources.go).
func buildFeeds(cfg config.Config, log *slog.Logger, m *metrics.Metrics) ([]app.Feed, error) {
	var feeds []app.Feed
	for _, src := range sources {
		f := cfg.Feed(src.Name)
		if f == nil || !f.On() {
			continue
		}
		name := src.Name
		feed, err := src.build(*f, wsconn.DefaultConfig(), log, func() { m.DecodeError(name) })
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		feeds = append(feeds, feed)
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

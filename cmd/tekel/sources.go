package main

import (
	"log/slog"

	"github.com/Amadeus-22/tekel/internal/adapter/binance"
	"github.com/Amadeus-22/tekel/internal/adapter/klever"
	"github.com/Amadeus-22/tekel/internal/adapter/kraken"
	"github.com/Amadeus-22/tekel/internal/adapter/mercadobitcoin"
	"github.com/Amadeus-22/tekel/internal/adapter/wsconn"
	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/platform/config"
)

// newFeed builds one adapter from its configured feed. conn and log come from
// the wiring; decodeErrors counts malformed messages for the source.
type newFeed func(f config.Feed, conn wsconn.Config, log *slog.Logger, decodeErrors func()) (app.Feed, error)

// source is one exchange or chain: its environment variables and how to
// build its adapter. This table is the only place that lists them; adding a
// source is a new package under internal/adapter plus one row here.
type source struct {
	config.Source
	build newFeed
}

var sources = []source{
	{config.Source{
		Name:   binance.Exchange,
		URLKey: "TEKEL_BINANCE_URL", URLDefault: binance.DefaultBaseURL,
		SymbolsKey: "TEKEL_BINANCE_SYMBOLS", SymbolsDefault: "BTCUSDT,ETHUSDT",
	}, func(f config.Feed, conn wsconn.Config, log *slog.Logger, decodeErrors func()) (app.Feed, error) {
		return binance.New(binance.Config{BaseURL: f.URL, Symbols: f.Symbols, Conn: conn, Logger: log, DecodeErrors: decodeErrors})
	}},
	{config.Source{
		Name:   kraken.Exchange,
		URLKey: "TEKEL_KRAKEN_URL", URLDefault: kraken.DefaultURL,
		SymbolsKey: "TEKEL_KRAKEN_SYMBOLS", SymbolsDefault: "BTC/USD,ETH/USD",
	}, func(f config.Feed, conn wsconn.Config, log *slog.Logger, decodeErrors func()) (app.Feed, error) {
		return kraken.New(kraken.Config{URL: f.URL, Symbols: f.Symbols, Conn: conn, Logger: log, DecodeErrors: decodeErrors})
	}},
	// Off unless symbols are set: LEAN needs a custom market to read its
	// output (docs/adr/0006), and the endpoints answer 403 to automated
	// clients since 2026-11 (docs/mercadobitcoin.md).
	{config.Source{
		Name:   mercadobitcoin.Exchange,
		URLKey: "TEKEL_MERCADOBITCOIN_URL", URLDefault: mercadobitcoin.DefaultURL,
		SymbolsKey: "TEKEL_MERCADOBITCOIN_SYMBOLS",
	}, func(f config.Feed, conn wsconn.Config, log *slog.Logger, decodeErrors func()) (app.Feed, error) {
		return mercadobitcoin.New(mercadobitcoin.Config{URL: f.URL, Symbols: f.Symbols, Conn: conn, Logger: log, DecodeErrors: decodeErrors})
	}},
	// The block feed's one "symbol" is the network ("mainnet" or "testnet");
	// an empty URL means that network's public node (docs/adr/0007).
	{config.Source{
		Name:       klever.Chain,
		URLKey:     "TEKEL_KLEVER_URL",
		SymbolsKey: "TEKEL_KLEVER_NETWORK", Single: true,
	}, func(f config.Feed, conn wsconn.Config, log *slog.Logger, decodeErrors func()) (app.Feed, error) {
		return klever.New(klever.Config{URL: f.URL, Network: f.Symbols[0], Conn: conn, Logger: log, DecodeErrors: decodeErrors})
	}},
}

// sourceConfigs is what config.Load needs from the table.
func sourceConfigs() []config.Source {
	out := make([]config.Source, len(sources))
	for i, s := range sources {
		out[i] = s.Source
	}
	return out
}

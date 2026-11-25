// Package metrics defines Tekel's Prometheus metrics on a private registry
// and implements app.Metrics.
package metrics

import (
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/reference"
)

// Metrics holds every Tekel metric. All methods are safe for concurrent use.
type Metrics struct {
	reg *prometheus.Registry

	trades       *prometheus.CounterVec
	blocks       *prometheus.CounterVec
	blockTxs     *prometheus.CounterVec
	incidents    *prometheus.CounterVec
	lag          *prometheus.HistogramVec
	connected    *prometheus.GaugeVec
	decodeErrors *prometheus.CounterVec
	backpressure *prometheus.CounterVec
	lateTrades   *prometheus.CounterVec
	sinkErrors   *prometheus.CounterVec
	alerts       *prometheus.CounterVec
	refFetches   *prometheus.CounterVec
	refDeviation *prometheus.GaugeVec
	refDistance  *prometheus.HistogramVec
	refCredits   *prometheus.GaugeVec
}

// New registers every metric, plus the Go runtime and process collectors, on a
// fresh registry (never the global one, so tests can create as many as needed).
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		trades: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_trades_total",
			Help: "Exchange trades received, including duplicates. Chain blocks are counted in tekel_blocks_total.",
		}, []string{"exchange", "symbol"}),
		blocks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_blocks_total",
			Help: "Chain blocks received, including duplicates.",
		}, []string{"chain", "network"}),
		blockTxs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_block_transactions_total",
			Help: "Transactions in the chain blocks received (the block's txCount), including duplicate blocks.",
		}, []string{"chain", "network"}),
		incidents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_incidents_total",
			Help: "Data-quality incidents detected, by kind.",
		}, []string{"exchange", "symbol", "kind"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tekel_feed_lag_seconds",
			Help:    "Received time minus source time of each trade, or of each block for chains (negative values clamp to 0).",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"exchange", "symbol"}),
		connected: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tekel_connected",
			Help: "1 while the exchange connection is up and subscribed.",
		}, []string{"exchange"}),
		decodeErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_decode_errors_total",
			Help: "Malformed exchange messages skipped.",
		}, []string{"exchange"}),
		backpressure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_backpressure_seconds_total",
			Help: "Time feeds spent blocked because the pipeline buffer was full.",
		}, []string{"exchange"}),
		lateTrades: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_late_trades_total",
			Help: "Trades that arrived after their minute bar was written and are not in any bar.",
		}, []string{"exchange", "symbol"}),
		sinkErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_sink_errors_total",
			Help: "Failed writes, by sink (incidents, bars).",
		}, []string{"sink"}),
		alerts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_alerts_total",
			Help: "Incident notifications by sender and outcome (sent, failed; dropped and limited have an empty sender).",
		}, []string{"sender", "outcome"}),
		refFetches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_reference_fetches_total",
			Help: "Reference price requests (CoinMarketCap) by outcome (ok, failed).",
		}, []string{"outcome"}),
		refDeviation: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tekel_reference_deviation_ratio",
			Help: "Last trade minus reference price, divided by the reference price (0.01 = 1% above).",
		}, []string{"exchange", "symbol"}),
		refDistance: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tekel_reference_distance_ratio",
			Help: "Absolute distance between a stream's last trade and the reference price at each check (0.001 = 0.1%). The share of checks under a bucket bound is the stream's agreement with CoinMarketCap.",
			// Bounds match the dashboard's agreement levels and the default 1% price_deviation threshold.
			Buckets: []float64{.0005, .001, .0025, .005, .01, .025},
		}, []string{"exchange", "symbol"}),
		refCredits: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tekel_reference_credits",
			Help: "API credits of the reference provider's key: used_today, used_month and left_month, as the provider reports them.",
		}, []string{"kind"}),
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.trades, m.blocks, m.blockTxs, m.incidents, m.lag, m.connected, m.decodeErrors, m.backpressure, m.lateTrades, m.sinkErrors, m.alerts,
		m.refFetches, m.refDeviation, m.refDistance, m.refCredits,
	)
	return m
}

// Handler serves the registry for GET /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// Registry exposes the registry, for tests and extra collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// TradeReceived implements app.Metrics.
func (m *Metrics) TradeReceived(exchange, symbol string, lag time.Duration) {
	m.trades.WithLabelValues(exchange, symbol).Inc()
	m.lag.WithLabelValues(exchange, symbol).Observe(max(lag.Seconds(), 0))
}

// BlockReceived implements app.Metrics. The lag goes to the same histogram as
// trades, labelled with the chain and network in the exchange and symbol labels.
func (m *Metrics) BlockReceived(chain, network string, txs int, lag time.Duration) {
	m.blocks.WithLabelValues(chain, network).Inc()
	m.blockTxs.WithLabelValues(chain, network).Add(float64(max(txs, 0)))
	m.lag.WithLabelValues(chain, network).Observe(max(lag.Seconds(), 0))
}

// IncidentRecorded implements app.Metrics.
func (m *Metrics) IncidentRecorded(exchange, symbol string, kind domain.IncidentKind) {
	m.incidents.WithLabelValues(exchange, symbol, string(kind)).Inc()
}

// LateTrade implements app.Metrics.
func (m *Metrics) LateTrade(exchange, symbol string) {
	m.lateTrades.WithLabelValues(exchange, symbol).Inc()
}

// SetConnected implements app.Metrics.
func (m *Metrics) SetConnected(exchange string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	m.connected.WithLabelValues(exchange).Set(v)
}

// Backpressure implements app.Metrics.
func (m *Metrics) Backpressure(exchange string, blocked time.Duration) {
	m.backpressure.WithLabelValues(exchange).Add(blocked.Seconds())
}

// SinkError implements app.Metrics.
func (m *Metrics) SinkError(sink string) { m.sinkErrors.WithLabelValues(sink).Inc() }

// DecodeError counts one malformed message; adapters receive it as a callback.
func (m *Metrics) DecodeError(exchange string) { m.decodeErrors.WithLabelValues(exchange).Inc() }

// Alert counts one incident notification outcome.
func (m *Metrics) Alert(sender, outcome string) { m.alerts.WithLabelValues(sender, outcome).Inc() }

// ReferenceFetch implements app.ReferenceMetrics.
func (m *Metrics) ReferenceFetch(ok bool) {
	outcome := "failed"
	if ok {
		outcome = "ok"
	}
	m.refFetches.WithLabelValues(outcome).Inc()
}

// ReferenceDeviation implements app.ReferenceMetrics.
func (m *Metrics) ReferenceDeviation(exchange, symbol string, deviation float64) {
	m.refDeviation.WithLabelValues(exchange, symbol).Set(deviation)
	m.refDistance.WithLabelValues(exchange, symbol).Observe(math.Abs(deviation))
}

// ReferenceUsage implements app.ReferenceMetrics.
func (m *Metrics) ReferenceUsage(u reference.Usage) {
	m.refCredits.WithLabelValues("used_today").Set(float64(u.UsedToday))
	m.refCredits.WithLabelValues("used_month").Set(float64(u.UsedMonth))
	m.refCredits.WithLabelValues("left_month").Set(float64(u.LeftMonth))
}

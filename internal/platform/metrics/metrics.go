// Package metrics defines Tekel's Prometheus metrics on a private registry
// and implements app.Metrics.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Amadeus-22/tekel/internal/domain"
)

// Metrics holds every Tekel metric. All methods are safe for concurrent use.
type Metrics struct {
	reg *prometheus.Registry

	trades       *prometheus.CounterVec
	incidents    *prometheus.CounterVec
	lag          *prometheus.HistogramVec
	connected    *prometheus.GaugeVec
	decodeErrors *prometheus.CounterVec
	backpressure *prometheus.CounterVec
	lateTrades   *prometheus.CounterVec
	sinkErrors   *prometheus.CounterVec
}

// New registers every metric, plus the Go runtime and process collectors, on a
// fresh registry (never the global one, so tests can create as many as needed).
func New() *Metrics {
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		trades: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_trades_total",
			Help: "Trades received, including duplicates.",
		}, []string{"exchange", "symbol"}),
		incidents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tekel_incidents_total",
			Help: "Data-quality incidents detected, by kind.",
		}, []string{"exchange", "symbol", "kind"}),
		lag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "tekel_feed_lag_seconds",
			Help:    "Received time minus exchange time of each trade (negative values clamp to 0).",
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
	}
	m.reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.trades, m.incidents, m.lag, m.connected, m.decodeErrors, m.backpressure, m.lateTrades, m.sinkErrors,
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

// Package httpapi serves tickguard's HTTP API: /v1 resources, health checks
// and Prometheus metrics.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

// Query limits for GET /v1/incidents.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

// StreamSource reports live stream state; app.Pipeline implements it.
type StreamSource interface {
	Streams() []app.StreamStatus
}

// IncidentSource queries stored incidents; the SQLite store implements it.
type IncidentSource interface {
	List(ctx context.Context, f app.IncidentFilter) ([]domain.Incident, error)
}

// Pinger checks a dependency for /readyz.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Deps are what the handlers read from.
type Deps struct {
	Streams   StreamSource
	Incidents IncidentSource
	Ready     Pinger
	Metrics   http.Handler
	Logger    *slog.Logger
}

// NewHandler returns the routed API.
func NewHandler(d Deps) http.Handler {
	h := &handler{d: d}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.HandleFunc("GET /readyz", h.readyz)
	mux.Handle("GET /metrics", d.Metrics)
	mux.HandleFunc("GET /v1/streams", h.streams)
	mux.HandleFunc("GET /v1/incidents", h.incidents)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "no route for "+r.Method+" "+r.URL.Path)
	})
	return mux
}

// NewServer wraps h in an http.Server with explicit timeouts.
func NewServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

type handler struct{ d Deps }

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.d.Ready.Ping(ctx); err != nil {
		h.d.Logger.Warn("readiness check failed", "error", err.Error())
		writeError(w, http.StatusServiceUnavailable, "not_ready", "incident store unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type streamJSON struct {
	Exchange         string         `json:"exchange"`
	Symbol           string         `json:"symbol"`
	Connected        bool           `json:"connected"`
	Trades           uint64         `json:"trades"`
	LastTradeID      int64          `json:"last_trade_id,omitzero"`
	LastPrice        string         `json:"last_price,omitzero"`
	LastExchangeTime time.Time      `json:"last_exchange_time,omitzero"`
	LastReceivedTime time.Time      `json:"last_received_time,omitzero"`
	LagSeconds       float64        `json:"lag_seconds"`
	Incidents        map[string]int `json:"incidents"`
}

func (h *handler) streams(w http.ResponseWriter, _ *http.Request) {
	all := h.d.Streams.Streams()
	out := make([]streamJSON, 0, len(all))
	for _, s := range all {
		counts := make(map[string]int, len(s.Incidents))
		for k, n := range s.Incidents {
			counts[string(k)] = n
		}
		out = append(out, streamJSON{
			Exchange: s.Exchange, Symbol: s.Symbol, Connected: s.Connected, Trades: s.Trades,
			LastTradeID: s.LastTradeID, LastPrice: s.LastPrice,
			LastExchangeTime: s.LastExchangeTime, LastReceivedTime: s.LastReceivedTime,
			LagSeconds: s.Lag.Seconds(), Incidents: counts,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"streams": out})
}

type incidentJSON struct {
	ID         int64     `json:"id"`
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	Kind       string    `json:"kind"`
	DetectedAt time.Time `json:"detected_at"`
	Start      time.Time `json:"start,omitzero"`
	End        time.Time `json:"end,omitzero"`
	TradeID    int64     `json:"trade_id,omitzero"`
	Detail     string    `json:"detail"`
}

func (h *handler) incidents(w http.ResponseWriter, r *http.Request) {
	f, err := parseFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_parameter", err.Error())
		return
	}
	list, err := h.d.Incidents.List(r.Context(), f)
	if err != nil {
		h.d.Logger.Error("list incidents", "error", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not read incidents")
		return
	}
	out := make([]incidentJSON, 0, len(list))
	for _, i := range list {
		out = append(out, incidentJSON{
			ID: i.ID, Exchange: i.Exchange, Symbol: i.Symbol, Kind: string(i.Kind),
			DetectedAt: i.DetectedAt, Start: i.Start, End: i.End, TradeID: i.TradeID, Detail: i.Detail,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"incidents": out})
}

func parseFilter(r *http.Request) (app.IncidentFilter, error) {
	q := r.URL.Query()
	f := app.IncidentFilter{Exchange: q.Get("exchange"), Symbol: q.Get("symbol"), Limit: DefaultLimit}
	if k := q.Get("kind"); k != "" {
		kind, err := domain.ParseIncidentKind(k)
		if err != nil {
			return f, errors.New("kind must be one of sequence_gap, duplicate, out_of_order, clock_skew, price_spike, stale_stream, disconnect")
		}
		f.Kind = kind
	}
	if s := q.Get("since"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return f, errors.New("since must be an RFC 3339 timestamp, e.g. 2026-01-02T15:04:05Z")
		}
		f.Since = t
	}
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 || n > MaxLimit {
			return f, errors.New("limit must be an integer between 1 and " + strconv.Itoa(MaxLimit))
		}
		f.Limit = n
	}
	return f, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v) // the client went away; nothing left to do
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	type body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	writeJSON(w, status, map[string]body{"error": {Code: code, Message: msg}})
}

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

type fakeStreams []app.StreamStatus

func (f fakeStreams) Streams() []app.StreamStatus { return f }

type fakeIncidents struct {
	got  app.IncidentFilter
	list []domain.Incident
	err  error
}

func (f *fakeIncidents) List(_ context.Context, filter app.IncidentFilter) ([]domain.Incident, error) {
	f.got = filter
	return f.list, f.err
}

type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

func newServer(t *testing.T, inc *fakeIncidents, ready error) *httptest.Server {
	t.Helper()
	h := NewHandler(Deps{
		Streams: fakeStreams{{
			Exchange: "binance", Symbol: "BTCUSDT", Kind: domain.StreamTrades, Connected: true, Trades: 42, LastTradeID: 9,
			LastPrice: "100.5", LastExchangeTime: t0, LastReceivedTime: t0.Add(30 * time.Millisecond),
			Lag: 30 * time.Millisecond, Incidents: map[domain.IncidentKind]int{domain.KindDuplicate: 2},
		}, {Exchange: "klever", Symbol: "testnet", Kind: domain.StreamBlocks, Incidents: map[domain.IncidentKind]int{}}},
		Incidents: inc,
		Ready:     fakePinger{err: ready},
		Metrics:   http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "metrics") }),
		Logger:    slog.New(slog.DiscardHandler),
	})
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if resp.Header.Get("Content-Type") == "application/json" {
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
	}
	return resp.StatusCode, body
}

func errorCode(body map[string]any) string {
	e, _ := body["error"].(map[string]any)
	code, _ := e["code"].(string)
	return code
}

func TestHealthAndReadiness(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	if code, body := get(t, srv.URL+"/healthz"); code != 200 || body["status"] != "ok" {
		t.Errorf("healthz = %d %v", code, body)
	}
	if code, body := get(t, srv.URL+"/readyz"); code != 200 || body["status"] != "ready" {
		t.Errorf("readyz = %d %v", code, body)
	}

	down := newServer(t, &fakeIncidents{}, errors.New("disk gone"))
	if code, body := get(t, down.URL+"/readyz"); code != 503 || errorCode(body) != "not_ready" {
		t.Errorf("readyz when down = %d %v", code, body)
	}
}

func TestMetricsRoute(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "metrics" {
		t.Errorf("/metrics = %q", b)
	}
}

func TestStreams(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	code, body := get(t, srv.URL+"/v1/streams")
	if code != 200 {
		t.Fatalf("status = %d", code)
	}
	streams := body["streams"].([]any)
	if len(streams) != 2 {
		t.Fatalf("streams = %v", streams)
	}
	s := streams[0].(map[string]any)
	if s["exchange"] != "binance" || s["kind"] != "trades" || s["connected"] != true || s["trades"] != float64(42) ||
		s["last_price"] != "100.5" || s["lag_seconds"] != 0.03 || s["last_exchange_time"] != "2026-03-01T12:00:00Z" {
		t.Errorf("stream = %v", s)
	}
	if s["incidents"].(map[string]any)["duplicate"] != float64(2) {
		t.Errorf("incident counts = %v", s["incidents"])
	}
	idle := streams[1].(map[string]any)
	if idle["kind"] != "blocks" {
		t.Errorf("block stream kind = %v", idle["kind"])
	}
	if _, ok := idle["last_exchange_time"]; ok {
		t.Errorf("a stream without trades must omit last_exchange_time: %v", idle)
	}
}

func TestIncidents(t *testing.T) {
	inc := &fakeIncidents{list: []domain.Incident{{
		ID: 3, Exchange: "kraken", Symbol: "BTC/USD", Kind: domain.KindDisconnect,
		DetectedAt: t0, Start: t0.Add(-time.Minute), End: t0, Detail: "down",
	}}}
	srv := newServer(t, inc, nil)
	code, body := get(t, srv.URL+"/v1/incidents?exchange=kraken&symbol=BTC/USD&kind=disconnect&since=2026-03-01T00:00:00Z&limit=5")
	if code != 200 {
		t.Fatalf("status = %d %v", code, body)
	}
	want := app.IncidentFilter{Exchange: "kraken", Symbol: "BTC/USD", Kind: domain.KindDisconnect,
		Since: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), Limit: 5}
	if inc.got != want {
		t.Errorf("filter = %+v, want %+v", inc.got, want)
	}
	list := body["incidents"].([]any)
	i := list[0].(map[string]any)
	if i["id"] != float64(3) || i["kind"] != "disconnect" || i["start"] != "2026-03-01T11:59:00Z" {
		t.Errorf("incident = %v", i)
	}
	if _, ok := i["trade_id"]; ok {
		t.Error("trade_id must be omitted when zero")
	}
}

func TestIncidentsDefaultsAndEmpty(t *testing.T) {
	inc := &fakeIncidents{}
	srv := newServer(t, inc, nil)
	code, body := get(t, srv.URL+"/v1/incidents")
	if code != 200 || inc.got.Limit != DefaultLimit {
		t.Errorf("status %d, limit %d", code, inc.got.Limit)
	}
	if list, ok := body["incidents"].([]any); !ok || len(list) != 0 {
		t.Errorf("incidents = %v, want an empty JSON array", body["incidents"])
	}
}

func TestIncidentsRejectsBadParameters(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	for _, q := range []string{"kind=gap", "since=yesterday", "limit=0", "limit=1001", "limit=x"} {
		code, body := get(t, srv.URL+"/v1/incidents?"+q)
		if code != 400 || errorCode(body) != "invalid_parameter" {
			t.Errorf("?%s = %d %v", q, code, body)
		}
	}
}

func TestIncidentsStoreFailure(t *testing.T) {
	srv := newServer(t, &fakeIncidents{err: errors.New("locked")}, nil)
	code, body := get(t, srv.URL+"/v1/incidents")
	if code != 500 || errorCode(body) != "internal" {
		t.Errorf("= %d %v", code, body)
	}
	if strings.Contains(body["error"].(map[string]any)["message"].(string), "locked") {
		t.Error("internal error details leaked to the client")
	}
}

func TestUnknownRoute(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	if code, body := get(t, srv.URL+"/v2/nothing"); code != 404 || errorCode(body) != "not_found" {
		t.Errorf("= %d %v", code, body)
	}
}

func TestNewServerSetsTimeouts(t *testing.T) {
	s := NewServer(":0", http.NotFoundHandler())
	if s.ReadHeaderTimeout == 0 || s.ReadTimeout == 0 || s.WriteTimeout == 0 || s.IdleTimeout == 0 {
		t.Errorf("server timeouts = %+v", s)
	}
}

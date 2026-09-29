package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain"
)

var t0 = time.Date(2026, 3, 1, 12, 0, 0, 123456000, time.UTC)

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "tickguard.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seed(t *testing.T, s *Store) {
	t.Helper()
	incs := []domain.Incident{
		{Exchange: "binance", Symbol: "BTCUSDT", Kind: domain.KindSequenceGap, DetectedAt: t0,
			Start: t0.Add(-time.Second), End: t0, TradeID: 105, Detail: "4 missing"},
		{Exchange: "binance", Symbol: "BTCUSDT", Kind: domain.KindDuplicate, DetectedAt: t0.Add(time.Minute), TradeID: 7, Detail: "dup"},
		{Exchange: "binance", Symbol: "ETHUSDT", Kind: domain.KindClockSkew, DetectedAt: t0.Add(2 * time.Minute), TradeID: 1, Detail: "skew"},
		{Exchange: "kraken", Symbol: "BTC/USD", Kind: domain.KindDisconnect, DetectedAt: t0.Add(3 * time.Minute),
			Start: t0.Add(2 * time.Minute), End: t0.Add(3 * time.Minute), Detail: "down"},
	}
	for _, inc := range incs {
		if err := s.Save(context.Background(), inc); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSaveAndListRoundTrip(t *testing.T) {
	s := openTemp(t)
	seed(t, s)
	got, err := s.List(context.Background(), app.IncidentFilter{Kind: domain.KindSequenceGap})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("List = %d incidents", len(got))
	}
	inc := got[0]
	if inc.ID == 0 || inc.Exchange != "binance" || inc.Symbol != "BTCUSDT" || inc.TradeID != 105 || inc.Detail != "4 missing" {
		t.Errorf("incident = %+v", inc)
	}
	if !inc.DetectedAt.Equal(t0) || !inc.Start.Equal(t0.Add(-time.Second)) || !inc.End.Equal(t0) {
		t.Errorf("times = %v %v %v (microsecond precision must survive)", inc.DetectedAt, inc.Start, inc.End)
	}

	got, _ = s.List(context.Background(), app.IncidentFilter{Kind: domain.KindDuplicate})
	if !got[0].Start.IsZero() || !got[0].End.IsZero() {
		t.Errorf("point incident has a window: %+v", got[0])
	}
	got, _ = s.List(context.Background(), app.IncidentFilter{Kind: domain.KindDisconnect})
	if got[0].TradeID != 0 {
		t.Errorf("disconnect trade id = %d, want 0", got[0].TradeID)
	}
}

func TestListFilters(t *testing.T) {
	s := openTemp(t)
	seed(t, s)
	tests := []struct {
		name string
		f    app.IncidentFilter
		want []domain.IncidentKind // newest first
	}{
		{"all", app.IncidentFilter{}, []domain.IncidentKind{domain.KindDisconnect, domain.KindClockSkew, domain.KindDuplicate, domain.KindSequenceGap}},
		{"exchange", app.IncidentFilter{Exchange: "binance"}, []domain.IncidentKind{domain.KindClockSkew, domain.KindDuplicate, domain.KindSequenceGap}},
		{"symbol", app.IncidentFilter{Exchange: "binance", Symbol: "BTCUSDT"}, []domain.IncidentKind{domain.KindDuplicate, domain.KindSequenceGap}},
		{"since is inclusive", app.IncidentFilter{Since: t0.Add(2 * time.Minute)}, []domain.IncidentKind{domain.KindDisconnect, domain.KindClockSkew}},
		{"limit", app.IncidentFilter{Limit: 1}, []domain.IncidentKind{domain.KindDisconnect}},
		{"no match", app.IncidentFilter{Symbol: "DOGEUSDT"}, []domain.IncidentKind{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.List(context.Background(), tt.f)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("List returned nil; want an empty slice for JSON []")
			}
			if len(got) != len(tt.want) {
				t.Fatalf("List = %d incidents, want %d", len(got), len(tt.want))
			}
			for i, k := range tt.want {
				if got[i].Kind != k {
					t.Errorf("[%d] = %s, want %s", i, got[i].Kind, k)
				}
			}
		})
	}
}

func TestReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tickguard.db")
	s, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	seed(t, s)
	s.Close()

	s, err = Open(context.Background(), path) // schema is idempotent
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.List(context.Background(), app.IncidentFilter{})
	if len(got) != 4 {
		t.Errorf("after reopen: %d incidents, want 4", len(got))
	}
}

func TestErrorsAfterClose(t *testing.T) {
	s := openTemp(t)
	s.Close()
	ctx := context.Background()
	if err := s.Ping(ctx); err == nil {
		t.Error("Ping after Close = nil")
	}
	if err := s.Save(ctx, domain.Incident{Kind: domain.KindDuplicate}); err == nil {
		t.Error("Save after Close = nil")
	}
	if _, err := s.List(ctx, app.IncidentFilter{}); err == nil {
		t.Error("List after Close = nil")
	}
}

func TestOpenFailsOnBadPath(t *testing.T) {
	if _, err := Open(context.Background(), filepath.Join(t.TempDir(), "missing", "dir", "x.db")); err == nil {
		t.Error("Open in a missing directory = nil error")
	}
}

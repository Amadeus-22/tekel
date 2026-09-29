package logging

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestNewWritesJSONAtLevel(t *testing.T) {
	var buf bytes.Buffer
	log, err := New(&buf, "warn")
	if err != nil {
		t.Fatal(err)
	}
	log.Info("hidden")
	log.Warn("shown", "trade_id", 7)

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not one JSON line: %q", buf.String())
	}
	if rec["msg"] != "shown" || rec["level"] != "WARN" || rec["trade_id"] != float64(7) {
		t.Errorf("record = %v", rec)
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	if _, err := New(&bytes.Buffer{}, "loud"); err == nil {
		t.Error("New(loud) = nil error")
	}
}

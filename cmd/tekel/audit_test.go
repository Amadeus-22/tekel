package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Amadeus-22/tekel/internal/app"
)

// fixtures are the LEAN sample zips described in internal/adapter/auditreport.
var fixtures = filepath.Join("..", "..", "internal", "adapter", "auditreport", "testdata", "lean")

func runAuditArgs(args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = runAudit(context.Background(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestAuditCommandEndToEnd(t *testing.T) {
	// Flags after the path, JSON for CI: the fixtures hold errors, so exit 1.
	code, out, stderr := runAuditArgs(fixtures, "--format", "json")
	if code != exitIssues || stderr != "" {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	var r app.AuditReport
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		t.Fatalf("stdout is not JSON: %v", err)
	}
	if len(r.Files) != 8 || r.Totals.Error == 0 || len(r.Skipped) != 1 {
		t.Errorf("report: %d files, totals %+v, skipped %d", len(r.Files), r.Totals, len(r.Skipped))
	}

	// One clean file (the trimmed Binance hour sample), table output.
	clean := filepath.Join(fixtures, "crypto", "binance", "hour", "btcusdt_trade.zip")
	code, out, _ = runAuditArgs("--fail-on", "warn", clean)
	if code != exitOK || !strings.Contains(out, "crypto/binance/hour/btcusdt") || !strings.Contains(out, "TOTAL files=1 rows=30 error=0 warn=0") {
		t.Errorf("clean file: exit %d\n%s", code, out)
	}

	// Warnings only: fail on warn, pass on error.
	warnOnly := filepath.Join(fixtures, "crypto", "coinbase", "minute", "btcusd")
	if code, _, _ := runAuditArgs(warnOnly); code != exitOK {
		t.Errorf("warn-only folder with --fail-on error: exit %d", code)
	}
	if code, _, _ := runAuditArgs(warnOnly, "--fail-on=warn"); code != exitIssues {
		t.Errorf("warn-only folder with --fail-on warn: exit %d", code)
	}
}

func TestAuditCommandUsageErrors(t *testing.T) {
	tests := []struct {
		args []string
		msg  string
	}{
		{nil, "usage: tekel audit"},
		{[]string{fixtures, "extra"}, "usage: tekel audit"},
		{[]string{fixtures, "--format", "xml"}, "--format must be table or json"},
		{[]string{fixtures, "--fail-on", "info"}, "--fail-on must be warn or error"},
		{[]string{"--nope", fixtures}, "flag provided but not defined"},
		{[]string{filepath.Join(fixtures, "does-not-exist")}, "no such file"},
	}
	for _, tt := range tests {
		code, _, stderr := runAuditArgs(tt.args...)
		if code != exitUsageErr || !strings.Contains(stderr, tt.msg) {
			t.Errorf("audit %v: exit %d, stderr %q", tt.args, code, stderr)
		}
	}
}

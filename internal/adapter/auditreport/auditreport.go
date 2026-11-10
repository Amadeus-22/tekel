// Package auditreport renders an app.AuditReport as a human-readable table or
// as JSON for CI.
package auditreport

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/Amadeus-22/tekel/internal/app"
	"github.com/Amadeus-22/tekel/internal/domain/audit"
)

// MaxTableIssues caps the issue lines of the table; JSON lists every kept issue.
const MaxTableIssues = 100

// Formats accepted by Write.
const (
	FormatTable = "table"
	FormatJSON  = "json"
)

// Write renders r in format (table or json).
func Write(w io.Writer, format string, r app.AuditReport) error {
	switch format {
	case FormatTable:
		return WriteTable(w, r)
	case FormatJSON:
		return WriteJSON(w, r)
	}
	return fmt.Errorf("unknown format %q (want table or json)", format)
}

// WriteJSON writes r as indented JSON.
func WriteJSON(w io.Writer, r app.AuditReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return nil
}

// WriteTable writes the per-series and per-file summaries, the warn and error
// issues (info issues are counted, and listed in JSON), skipped files and totals.
func WriteTable(w io.Writer, r app.AuditReport) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	p := func(format string, a ...any) { fmt.Fprintf(tw, format, a...) }

	p("SERIES\tTZ\tFILES\tROWS\tFIRST\tLAST\tERROR\tWARN\tINFO\n")
	for _, s := range r.Series {
		p("%s\t%s\t%d\t%d\t%s\t%s\t%d\t%d\t%d\n", s.Series, s.TimeZone, s.Files, s.Rows, dash(s.First), dash(s.Last), s.Error, s.Warn, s.Info)
	}
	p("\nFILE\tROWS\tFIRST\tLAST\tERROR\tWARN\tINFO\n")
	for _, f := range r.Files {
		p("%s\t%d\t%s\t%s\t%d\t%d\t%d\n", f.File, f.Rows, dash(f.First), dash(f.Last), f.Error, f.Warn, f.Info)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	var shown []app.Issue
	for _, is := range r.Issues {
		if is.Severity.Rank() >= audit.Warn.Rank() && len(shown) < MaxTableIssues {
			shown = append(shown, is)
		}
	}
	serious := r.Totals.Error + r.Totals.Warn
	p("\nISSUES (error and warn: %d, showing %d)\n", serious, len(shown))
	for _, is := range shown {
		loc := is.File
		if is.Line > 0 {
			loc = fmt.Sprintf("%s:%d", is.File, is.Line)
		}
		p("%s\t%s\t%s\t%s\n", loc, is.Severity, is.Kind, is.Detail)
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	if len(r.Skipped) > 0 {
		reasons := map[string]int{}
		for _, s := range r.Skipped {
			reasons[s.Reason]++
		}
		var parts []string
		for _, k := range slices.Sorted(maps.Keys(reasons)) {
			parts = append(parts, fmt.Sprintf("%d × %s", reasons[k], k))
		}
		p("\nSKIPPED %d file(s): %s\n", len(r.Skipped), strings.Join(parts, "; "))
	}
	p("\nTOTAL files=%d rows=%d error=%d warn=%d info=%d", len(r.Files), r.Rows, r.Totals.Error, r.Totals.Warn, r.Totals.Info)
	if r.IssuesOmitted > 0 {
		p(" (%d issues beyond the cap not listed)", r.IssuesOmitted)
	}
	p("\n")
	return tw.Flush()
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

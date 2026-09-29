package main

import (
	"context"
	"flag"
	"fmt"
	"io"

	"github.com/Amadeus-22/tickguard/internal/adapter/auditreport"
	"github.com/Amadeus-22/tickguard/internal/adapter/leanfs"
	"github.com/Amadeus-22/tickguard/internal/app"
	"github.com/Amadeus-22/tickguard/internal/domain/audit"
)

// Exit codes of "tickguard audit".
const (
	exitOK       = 0
	exitIssues   = 1 // issues at or above --fail-on
	exitUsageErr = 2 // bad arguments or unreadable path
)

const auditUsage = `usage: tickguard audit <path> [--format table|json] [--fail-on warn|error]

Audits LEAN trade data under <path> (a data folder, any sub-folder, or one zip).
Exit status: 0 clean, 1 issues at or above --fail-on, 2 usage or I/O error.
`

// runAudit is the "audit" subcommand. Flags may come before or after the path.
func runAudit(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("audit", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, auditUsage); fs.PrintDefaults() }
	format := fs.String("format", auditreport.FormatTable, "output format: table or json")
	failOn := fs.String("fail-on", string(audit.Error), "exit 1 when an issue of this severity or worse exists: warn or error")

	if err := fs.Parse(args); err != nil {
		return exitUsageErr
	}
	path := fs.Arg(0)
	if path != "" {
		if err := fs.Parse(fs.Args()[1:]); err != nil {
			return exitUsageErr
		}
	}
	if path == "" || fs.NArg() > 0 {
		fs.Usage()
		return exitUsageErr
	}
	threshold, err := audit.ParseSeverity(*failOn)
	if err != nil || threshold == audit.Info {
		fmt.Fprintf(stderr, "tickguard audit: --fail-on must be warn or error, got %q\n", *failOn)
		return exitUsageErr
	}
	if *format != auditreport.FormatTable && *format != auditreport.FormatJSON {
		fmt.Fprintf(stderr, "tickguard audit: --format must be table or json, got %q\n", *format)
		return exitUsageErr
	}

	src, err := leanfs.NewSource(path)
	if err != nil {
		fmt.Fprintln(stderr, "tickguard audit:", err)
		return exitUsageErr
	}
	report, err := app.Audit(ctx, src, app.DefaultAuditConfig())
	if err != nil {
		fmt.Fprintln(stderr, "tickguard audit:", err)
		return exitUsageErr
	}
	if err := auditreport.Write(stdout, *format, report); err != nil {
		fmt.Fprintln(stderr, "tickguard audit:", err)
		return exitUsageErr
	}
	if report.Fails(threshold) {
		return exitIssues
	}
	return exitOK
}

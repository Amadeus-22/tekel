package app

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain/audit"
)

// SourceFile is one file found under the audited path.
type SourceFile struct {
	// Name is the path shown in reports: relative to the audited directory.
	Name string
	// Path is the absolute, slash-separated path, used to recognise the layout.
	Path string
}

// LeanSource lists and opens the files of a LEAN data folder. The filesystem
// implementation lives in internal/adapter/leanfs; tests use a fake.
type LeanSource interface {
	// Files returns every zip file under the audited path, sorted by Name.
	Files(ctx context.Context) ([]SourceFile, error)
	// Open returns the CSV inside the zip. It fails when the zip is unreadable
	// or does not hold exactly one entry.
	Open(ctx context.Context, f SourceFile) (io.ReadCloser, error)
}

// AuditConfig tunes an audit run.
type AuditConfig struct {
	Checks audit.Config
	// MaxIssues caps the issues kept in the report; counts stay exact.
	MaxIssues int
}

// DefaultAuditConfig keeps up to 10 000 issues.
func DefaultAuditConfig() AuditConfig {
	return AuditConfig{Checks: audit.DefaultConfig(), MaxIssues: 10_000}
}

// Counts tallies findings by severity.
type Counts struct {
	Error int `json:"error"`
	Warn  int `json:"warn"`
	Info  int `json:"info"`
}

func (c *Counts) add(s audit.Severity) {
	switch s {
	case audit.Error:
		c.Error++
	case audit.Warn:
		c.Warn++
	case audit.Info:
		c.Info++
	}
}

// Worst returns the most serious severity counted, or "" when there is none.
func (c Counts) Worst() audit.Severity {
	switch {
	case c.Error > 0:
		return audit.Error
	case c.Warn > 0:
		return audit.Warn
	case c.Info > 0:
		return audit.Info
	}
	return ""
}

// Issue is a finding placed in its file.
type Issue struct {
	File     string         `json:"file"`
	Line     int            `json:"line,omitempty"`
	Series   string         `json:"series"`
	Kind     audit.Kind     `json:"kind"`
	Severity audit.Severity `json:"severity"`
	// Time is in the series' time zone (see SeriesSummary.TimeZone).
	Time   string `json:"time,omitempty"`
	Detail string `json:"detail"`
}

// FileSummary describes one audited file.
type FileSummary struct {
	File   string `json:"file"`
	Series string `json:"series"`
	Rows   int    `json:"rows"`
	First  string `json:"first,omitempty"`
	Last   string `json:"last,omitempty"`
	Counts
}

// SeriesSummary aggregates the files of one symbol at one resolution.
type SeriesSummary struct {
	Series   string `json:"series"`
	TimeZone string `json:"time_zone"`
	Files    int    `json:"files"`
	Rows     int    `json:"rows"`
	First    string `json:"first,omitempty"`
	Last     string `json:"last,omitempty"`
	Counts
}

// SkippedFile is a zip that is not an audited LEAN trade file.
type SkippedFile struct {
	File   string `json:"file"`
	Reason string `json:"reason"`
}

// AuditReport is the result of an audit run.
type AuditReport struct {
	Files   []FileSummary   `json:"files"`
	Series  []SeriesSummary `json:"series"`
	Issues  []Issue         `json:"issues"`
	Skipped []SkippedFile   `json:"skipped"`
	// IssuesOmitted counts issues beyond AuditConfig.MaxIssues.
	IssuesOmitted int    `json:"issues_omitted"`
	Rows          int    `json:"rows"`
	Totals        Counts `json:"totals"`
}

// Audit reads every LEAN trade file of src and reports its quality issues. Files
// are streamed row by row; memory grows with the number of files, not rows.
func Audit(ctx context.Context, src LeanSource, cfg AuditConfig) (AuditReport, error) {
	files, err := src.Files(ctx)
	if err != nil {
		return AuditReport{}, fmt.Errorf("list files: %w", err)
	}
	r := &reportBuilder{cfg: cfg, series: map[string]*seriesAcc{}}
	for _, sf := range files {
		if err := ctx.Err(); err != nil {
			return AuditReport{}, err
		}
		f, err := audit.ClassifyPath(sf.Path)
		if err != nil {
			r.report.Skipped = append(r.report.Skipped, SkippedFile{File: sf.Name, Reason: strings.TrimPrefix(
				err.Error(), audit.ErrUnsupported.Error()+": ")})
			continue
		}
		if err := r.auditFile(ctx, src, sf, f); err != nil {
			return AuditReport{}, err
		}
	}
	return r.finish(), nil
}

type seriesAcc struct {
	series      audit.Series
	summary     SeriesSummary
	first, last time.Time
	dates       []time.Time
	lastName    string // file used to place series-level findings
}

type reportBuilder struct {
	cfg    AuditConfig
	report AuditReport
	series map[string]*seriesAcc
}

func (r *reportBuilder) auditFile(ctx context.Context, src LeanSource, sf SourceFile, f audit.File) error {
	key := f.Series.String()
	acc := r.series[key]
	if acc == nil {
		acc = &seriesAcc{series: f.Series, summary: SeriesSummary{Series: key, TimeZone: f.TimeZone()}}
		r.series[key] = acc
	}
	acc.summary.Files++
	acc.lastName = sf.Name
	if f.Resolution.Dated() {
		acc.dates = append(acc.dates, f.Date)
	}

	fs := FileSummary{File: sf.Name, Series: key}
	var (
		first, last time.Time
		findings    []audit.Finding
	)
	add := func(fd audit.Finding) {
		fs.Counts.add(fd.Severity)
		acc.summary.Counts.add(fd.Severity)
		findings = append(findings, fd)
	}
	defer func() {
		// Spike verdicts arrive half a window late: list the file's issues by
		// line, file-level ones (line 0) last.
		slices.SortStableFunc(findings, func(a, b audit.Finding) int { return cmp.Compare(lineKey(a), lineKey(b)) })
		for _, fd := range findings {
			r.addIssue(sf.Name, key, fd)
		}
		r.report.Files = append(r.report.Files, fs)
	}()

	rc, err := src.Open(ctx, sf)
	if err != nil {
		add(audit.Finding{Kind: audit.KindBadFile, Severity: audit.Error, Detail: err.Error()})
		return nil
	}
	defer rc.Close()

	checker := audit.NewChecker(f, r.cfg.Checks)
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text()) // also drops the CR of CRLF files
		if text == "" {
			continue
		}
		var (
			t     time.Time
			found []audit.Finding
			perr  error
		)
		if f.Resolution == audit.Tick {
			var tk audit.TradeTick
			if tk, perr = audit.ParseTick(f, line, text); perr == nil {
				t, found = tk.Time, checker.Tick(tk)
			}
		} else {
			var b audit.Bar
			if b, perr = audit.ParseBar(f, line, text); perr == nil {
				t, found = b.Time, checker.Bar(b)
			}
		}
		fs.Rows++
		if perr != nil {
			add(audit.Finding{Line: line, Kind: audit.KindUnparseableRow, Severity: audit.Error, Detail: perr.Error()})
			continue
		}
		if first.IsZero() || t.Before(first) {
			first = t
		}
		if t.After(last) {
			last = t
		}
		for _, fd := range found {
			add(fd)
		}
	}
	if err := sc.Err(); err != nil {
		add(audit.Finding{Kind: audit.KindBadFile, Severity: audit.Error, Detail: fmt.Sprintf("read: %v", err)})
	}
	for _, fd := range checker.Finish() {
		add(fd)
	}

	fs.First, fs.Last = stampOrEmpty(first), stampOrEmpty(last)
	acc.summary.Rows += fs.Rows
	if !first.IsZero() && (acc.first.IsZero() || first.Before(acc.first)) {
		acc.first = first
	}
	if last.After(acc.last) {
		acc.last = last
	}
	r.report.Rows += fs.Rows
	return nil
}

func lineKey(f audit.Finding) int {
	if f.Line == 0 {
		return math.MaxInt
	}
	return f.Line
}

func (r *reportBuilder) addIssue(file, series string, fd audit.Finding) {
	r.report.Totals.add(fd.Severity)
	if len(r.report.Issues) >= r.cfg.MaxIssues {
		r.report.IssuesOmitted++
		return
	}
	r.report.Issues = append(r.report.Issues, Issue{
		File: file, Line: fd.Line, Series: series, Kind: fd.Kind, Severity: fd.Severity,
		Time: stampOrEmpty(fd.Time), Detail: fd.Detail,
	})
}

func (r *reportBuilder) finish() AuditReport {
	keys := make([]string, 0, len(r.series))
	for k := range r.series {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		acc := r.series[k]
		slices.SortFunc(acc.dates, func(a, b time.Time) int { return a.Compare(b) })
		for _, fd := range audit.CheckDays(acc.series, acc.dates) {
			acc.summary.Counts.add(fd.Severity)
			// A missing day belongs to the series; name its folder, not a file.
			dir := k
			if i := strings.LastIndex(acc.lastName, "/"); i > 0 {
				dir = acc.lastName[:i]
			}
			r.addIssue(dir, k, fd)
		}
		acc.summary.First, acc.summary.Last = stampOrEmpty(acc.first), stampOrEmpty(acc.last)
		r.report.Series = append(r.report.Series, acc.summary)
	}
	if r.report.Issues == nil {
		r.report.Issues = []Issue{}
	}
	if r.report.Skipped == nil {
		r.report.Skipped = []SkippedFile{}
	}
	return r.report
}

// Fails reports whether the report holds an issue at least as serious as threshold.
func (r AuditReport) Fails(threshold audit.Severity) bool {
	worst := r.Totals.Worst()
	return worst != "" && worst.Rank() >= threshold.Rank()
}

func stampOrEmpty(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02T15:04:05.999")
}

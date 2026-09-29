package audit

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	d1016      = time.Date(2016, 10, 7, 0, 0, 0, 0, time.UTC)
	cryptoMin  = File{Series: Series{Crypto, "coinbase", Minute, "btcusd"}, Date: d1016}
	cryptoTick = File{Series: Series{Crypto, "coinbase", Tick, "btcusd"}, Date: d1016}
	cryptoHour = File{Series: Series{Crypto, "binance", Hour, "btcusdt"}}
	equityMin  = File{Series: Series{Equity, "usa", Minute, "spy"}, Date: time.Date(2013, 10, 7, 0, 0, 0, 0, time.UTC)}
	atMin      = func(m int) time.Time { return d1016.Add(time.Duration(m) * time.Minute) }
	rat        = func(s string) *big.Rat { r, _ := new(big.Rat).SetString(s); return r }
	testConfig = Config{Spike: DefaultConfig().Spike, GapMin: time.Minute, GapWarn: time.Hour}
	kindsOf    = func(fs []Finding) (out []string) {
		for _, f := range fs {
			out = append(out, string(f.Kind)+"/"+string(f.Severity))
		}
		return out
	}
)

func bar(line, minute int, o, h, l, c, v string) Bar {
	return Bar{Line: line, Time: atMin(minute), Open: rat(o), High: rat(h), Low: rat(l), Close: rat(c), Volume: rat(v)}
}

func TestClassifyPath(t *testing.T) {
	tests := []struct {
		path    string
		want    File
		wantErr string
	}{
		{"Data/crypto/coinbase/minute/btcusd/20161007_trade.zip", cryptoMin, ""},
		{"crypto/coinbase/tick/btcusd/20161007_trade.zip", cryptoTick, ""},
		{"/x/crypto/binance/hour/btcusdt_trade.zip", cryptoHour, ""},
		{"crypto/coinbase/daily/btcusd_trade.zip", File{Series: Series{Crypto, "coinbase", Daily, "btcusd"}}, ""},
		{"equity/usa/minute/spy/20131007_trade.zip", equityMin, ""},
		{"crypto/coinbase/minute/btcusd/20161007_quote.zip", File{}, "quote data"},
		{"crypto/coinbase/daily/btcusd_quote.zip", File{}, "quote data"},
		{"equity/usa/daily/spy.zip", File{}, "file name"},
		{"equity/usa/tick/spy/20131007_trade.zip", File{}, "equity tick"},
		{"crypto/coinbase/hour/btcusd/20161007_trade.zip", File{}, "not a dated layout"},
		{"crypto/coinbase/minute/btcusd/20161399_trade.zip", File{}, "bad date"},
		{"20161007_trade.zip", File{}, "unsupported"},
		{"crypto/coinbase/minute/btcusd/readme.md", File{}, "file name"},
	}
	for _, tt := range tests {
		got, err := ClassifyPath(tt.path)
		if tt.wantErr != "" {
			if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("ClassifyPath(%q) err = %v, want %q", tt.path, err, tt.wantErr)
			}
			continue
		}
		if err != nil || got != tt.want {
			t.Errorf("ClassifyPath(%q) = %+v, %v; want %+v", tt.path, got, err, tt.want)
		}
	}
}

func TestParseBar(t *testing.T) {
	tests := []struct {
		name    string
		file    File
		text    string
		time    time.Time
		close   string
		wantErr string
	}{
		{"minute", cryptoMin, "60000,611.78,611.91,611.78,611.9,1.13872", atMin(1), "611.9", ""},
		{"hour", cryptoHour, "20180501 01:00,8955.13,9020.99,8870,8994.99,4347.876744",
			time.Date(2018, 5, 1, 1, 0, 0, 0, time.UTC), "8994.99", ""},
		{"equity deci-cents", equityMin, "14400000,1674100,1674100,1672500,1672700,3197",
			equityMin.Date.Add(4 * time.Hour), "167.27", ""},
		{"negative kept", cryptoMin, "0,1,1,1,1,-5", d1016, "1", ""},
		{"exponent", cryptoMin, "0,6.4e-05,6.4e-05,6.4e-05,6.4e-05,1", d1016, "0.000064", ""},
		{"too few fields", cryptoMin, "0,1,1,1,1", time.Time{}, "", "5 fields"},
		{"bad number", cryptoMin, "0,1,x,1,1,1", time.Time{}, "", "field 3"},
		{"fraction rejected", cryptoMin, "0,1/3,1,1,1,1", time.Time{}, "", "field 2"},
		{"huge exponent", cryptoMin, "0,1e999999999,1,1,1,1", time.Time{}, "", "field 2"},
		{"bad time", cryptoMin, "abc,1,1,1,1,1", time.Time{}, "", "time"},
		{"bad hour time", cryptoHour, "2018-05-01,1,1,1,1,1", time.Time{}, "", "yyyyMMdd HH:mm"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := ParseBar(tt.file, 7, tt.text)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if b.Line != 7 || !b.Time.Equal(tt.time) || b.Close.Cmp(rat(tt.close)) != 0 {
				t.Errorf("bar = line %d time %s close %s", b.Line, b.Time, b.Close.FloatString(6))
			}
		})
	}
}

func TestParseTick(t *testing.T) {
	tests := []struct {
		text       string
		time       time.Time
		suspicious bool
		wantErr    bool
	}{
		{"4000,611.78,0.6", d1016.Add(4 * time.Second), false, false},
		{"4000.5,611.78,0.6,1", d1016.Add(4000500 * time.Microsecond), true, false},
		{"4000,611.78,0.6,0", d1016.Add(4 * time.Second), false, false},
		{"4000,611.78", time.Time{}, false, true},
		{"4000,611.78,abc", time.Time{}, false, true},
		{"4000.0000001,1,1", time.Time{}, false, true}, // below a nanosecond
	}
	for _, tt := range tests {
		tk, err := ParseTick(cryptoTick, 1, tt.text)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseTick(%q) err = %v", tt.text, err)
			continue
		}
		if err == nil && (!tk.Time.Equal(tt.time) || tk.Suspicious != tt.suspicious) {
			t.Errorf("ParseTick(%q) = %+v", tt.text, tk)
		}
	}
}

func TestCheckTime(t *testing.T) {
	tests := []struct {
		name string
		file File
		t    time.Time
		bad  string
	}{
		{"start of day", cryptoMin, d1016, ""},
		{"last minute", cryptoMin, atMin(1439), ""},
		{"next day", cryptoMin, atMin(1440), "outside the file's day"},
		{"before day", cryptoMin, d1016.Add(-time.Minute), "outside the file's day"},
		{"misaligned minute", cryptoMin, d1016.Add(30 * time.Second), "not aligned"},
		{"tick any ms", cryptoTick, d1016.Add(1234567 * time.Microsecond), ""},
		{"hour aligned", cryptoHour, time.Date(2018, 5, 1, 3, 0, 0, 0, time.UTC), ""},
		{"hour misaligned", cryptoHour, time.Date(2018, 5, 1, 3, 15, 0, 0, time.UTC), "not aligned"},
	}
	for _, tt := range tests {
		f, bad := CheckTime(tt.file, 1, tt.t)
		if bad != (tt.bad != "") || (bad && (f.Kind != KindBadTime || !strings.Contains(f.Detail, tt.bad))) {
			t.Errorf("%s: CheckTime = %+v, %v", tt.name, f, bad)
		}
	}
}

func TestCheckOHLC(t *testing.T) {
	tests := []struct {
		o, h, l, c string
		bad        string
	}{
		{"10", "12", "9", "11", ""},
		{"10", "10", "10", "10", ""},
		{"10", "9", "12", "10", "high < low"},
		{"13", "12", "9", "11", "open outside"},
		{"8", "12", "9", "11", "open outside"},
		{"10", "12", "9", "12.5", "close outside"},
		{"10", "12", "9", "8.99", "close outside"},
	}
	for _, tt := range tests {
		f, bad := CheckOHLC(bar(3, 0, tt.o, tt.h, tt.l, tt.c, "1"))
		if bad != (tt.bad != "") || (bad && (f.Severity != Error || f.Line != 3 || !strings.Contains(f.Detail, tt.bad))) {
			t.Errorf("CheckOHLC(%v) = %+v, %v", tt, f, bad)
		}
	}
}

func TestCheckPricesAndVolume(t *testing.T) {
	tests := []struct {
		prices []string
		bad    bool
	}{
		{[]string{"1", "0.0001"}, false},
		{[]string{"1", "0"}, true},
		{[]string{"-1"}, true},
	}
	for _, tt := range tests {
		var ps []*big.Rat
		for _, p := range tt.prices {
			ps = append(ps, rat(p))
		}
		if f, bad := CheckPrices(1, d1016, ps...); bad != tt.bad || (bad && f.Kind != KindNonPositive) {
			t.Errorf("CheckPrices(%v) = %+v, %v", tt.prices, f, bad)
		}
	}
	for v, want := range map[string]bool{"0": false, "2.5": false, "-0.1": true} {
		if f, bad := CheckVolume(1, d1016, rat(v)); bad != want || (bad && f.Kind != KindNegativeVolume) {
			t.Errorf("CheckVolume(%s) = %+v, %v", v, f, bad)
		}
	}
}

func TestCheckerSequenceAndGaps(t *testing.T) {
	cfg := testConfig
	tests := []struct {
		name   string
		file   File
		bars   []Bar
		want   []string // kind/severity of every finding, in order, Finish included
		detail string   // substring of the first finding's detail
	}{
		{"full day is clean", cryptoMin, fullDay(), nil, ""},
		{"duplicate timestamp", cryptoMin, withDay(bar(2, 1, "1", "1", "1", "1", "1"), bar(3, 1, "1", "1", "1", "1", "1")),
			[]string{"duplicate_timestamp/error"}, "repeats line 2"},
		{"out of order", cryptoMin, withDay(bar(2, 5, "1", "1", "1", "1", "1"), bar(3, 3, "1", "1", "1", "1", "1")),
			[]string{"missing_bars/info", "out_of_order/error"}, "4 minute bar(s) missing"},
		{"short gap is info", cryptoMin, withDay(bar(2, 1, "1", "1", "1", "1", "1"), bar(3, 12, "1", "1", "1", "1", "1")),
			[]string{"missing_bars/info"}, "10 minute bar(s) missing"},
		{"long gap is warn", cryptoMin, withDay(bar(2, 1, "1", "1", "1", "1", "1"), bar(3, 200, "1", "1", "1", "1", "1")),
			[]string{"missing_bars/warn"}, "198 minute bar(s) missing"},
		{"leading and trailing gaps", cryptoMin, []Bar{bar(1, 30, "1", "1", "1", "1", "1"), bar(2, 31, "1", "1", "1", "1", "1")},
			[]string{"missing_bars/info", "missing_bars/warn"}, "from 2016-10-07 00:00:00 to 2016-10-07 00:30:00"},
		{"empty file is a missing day", cryptoMin, nil, []string{"missing_bars/warn"}, "1440 minute bar(s)"},
		{"equity gaps not reported", equityMin, []Bar{{Line: 1, Time: equityMin.Date.Add(4 * time.Hour),
			Open: rat("1"), High: rat("1"), Low: rat("1"), Close: rat("1"), Volume: rat("1")}}, nil, ""},
		{"bad time skips sequence", cryptoMin, withDay(bar(2, 1441, "1", "1", "1", "1", "1")),
			[]string{"bad_time/error"}, "outside"},
		{"row checks", cryptoMin, withDay(bar(2, 1, "1", "0.5", "1", "1", "-1")),
			[]string{"ohlc_inconsistent/error", "negative_volume/error"}, "high < low"}, // and no spike on top
		{"zero price", cryptoMin, withDay(bar(2, 1, "0", "1", "0", "1", "1")),
			[]string{"non_positive_price/error"}, "price 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewChecker(tt.file, cfg)
			var got []Finding
			for _, b := range tt.bars {
				got = append(got, c.Bar(b)...)
			}
			got = append(got, c.Finish()...)
			if strings.Join(kindsOf(got), " ") != strings.Join(tt.want, " ") {
				t.Fatalf("findings = %v, want %v\n%+v", kindsOf(got), tt.want, got)
			}
			if tt.detail != "" && !strings.Contains(got[0].Detail, tt.detail) {
				t.Errorf("detail = %q, want %q", got[0].Detail, tt.detail)
			}
		})
	}
}

func TestCheckerHourGaps(t *testing.T) {
	c := NewChecker(cryptoHour, testConfig)
	h := func(line, hour int) Bar {
		return Bar{Line: line, Time: time.Date(2018, 5, 1, hour, 0, 0, 0, time.UTC),
			Open: rat("1"), High: rat("1"), Low: rat("1"), Close: rat("1"), Volume: rat("1")}
	}
	got := append(c.Bar(h(1, 0)), c.Bar(h(2, 1))...)
	got = append(got, c.Bar(h(3, 4))...)
	got = append(got, c.Finish()...) // undated: no trailing gap
	if len(got) != 1 || got[0].Severity != Warn || !strings.Contains(got[0].Detail, "2 hour bar(s) missing") {
		t.Errorf("findings = %+v", got)
	}
}

func TestCheckerTicks(t *testing.T) {
	c := NewChecker(cryptoTick, testConfig)
	tk := func(line int, ms int64, price, qty string) TradeTick {
		return TradeTick{Line: line, Time: d1016.Add(time.Duration(ms) * time.Millisecond), Price: rat(price), Quantity: rat(qty)}
	}
	var got []Finding
	for _, row := range []TradeTick{
		tk(1, 0, "100", "1"),
		tk(2, 0, "100", "1"), // same ms: fine for ticks
		tk(3, 500, "100", "-1"),
		tk(4, 400, "100", "1"),
		tk(5, 90_000, "0", "1"),
	} {
		got = append(got, c.Tick(row)...)
	}
	want := "negative_volume/error out_of_order/error missing_bars/info non_positive_price/error"
	if strings.Join(kindsOf(got), " ") != want {
		t.Errorf("findings = %v\n%+v", kindsOf(got), got)
	}
	if !strings.Contains(got[2].Detail, "no trades") {
		t.Errorf("tick gap detail = %q", got[2].Detail)
	}
}

func TestCheckerSpike(t *testing.T) {
	c := NewChecker(cryptoMin, testConfig)
	p := func(m int) string { // 100 ± 0.1, then a genuine 3% level shift at minute 100
		base := 100.0
		if m >= 100 {
			base = 103
		}
		return strconv.FormatFloat(base+0.1*float64(m%2), 'f', 1, 64)
	}
	var got []Finding
	for m := range 200 {
		b := bar(m+1, m, p(m), p(m), p(m), p(m), "1")
		switch m {
		case 50: // wick: close is normal, the high is 3x the median
			b.High = rat("300")
		case 150: // the close itself jumps
			b.Open, b.High, b.Close = rat("250"), rat("250"), rat("250")
		}
		got = append(got, c.Bar(b)...)
	}
	got = append(got, c.Finish()...)
	var spikes []string
	for _, f := range got {
		if f.Kind == KindPriceSpike {
			spikes = append(spikes, fmt.Sprintf("%d:%s", f.Line, f.Severity))
		}
	}
	if strings.Join(spikes, " ") != "51:warn 151:warn" {
		t.Fatalf("spikes at %v, want lines 51 and 151 only (the level shift is not a spike)\n%+v", spikes, got)
	}
}

func TestCheckerSpikeNeedsNeighbours(t *testing.T) {
	c := NewChecker(cryptoMin, testConfig)
	var got []Finding
	for m := range 10 { // fewer rows than SpikeMinSamples: never judged
		p := "100"
		if m == 5 {
			p = "500"
		}
		got = append(got, c.Bar(bar(m+1, m, p, p, p, p, "1"))...)
	}
	for _, f := range append(got, c.Finish()...) {
		if f.Kind == KindPriceSpike {
			t.Errorf("spike judged without enough neighbours: %+v", f)
		}
	}
}

func TestCheckDays(t *testing.T) {
	d := func(day int) time.Time { return time.Date(2016, 10, day, 0, 0, 0, 0, time.UTC) }
	tests := []struct {
		name   string
		series Series
		dates  []time.Time
		want   []string
	}{
		{"consecutive", cryptoMin.Series, []time.Time{d(7), d(8), d(9)}, nil},
		{"one missing", cryptoMin.Series, []time.Time{d(7), d(9)}, []string{"1 day(s) without a file: 2016-10-08 to 2016-10-08"}},
		{"two runs", cryptoMin.Series, []time.Time{d(1), d(4), d(5), d(7)},
			[]string{"2 day(s) without a file: 2016-10-02 to 2016-10-03", "1 day(s) without a file: 2016-10-06 to 2016-10-06"}},
		{"equity skipped", equityMin.Series, []time.Time{d(7), d(10)}, nil},
		{"hour skipped", cryptoHour.Series, []time.Time{d(7), d(10)}, nil},
	}
	for _, tt := range tests {
		got := CheckDays(tt.series, tt.dates)
		var details []string
		for _, f := range got {
			if f.Kind != KindMissingDays || f.Severity != Warn || f.Line != 0 {
				t.Errorf("%s: finding %+v", tt.name, f)
			}
			details = append(details, f.Detail)
		}
		if strings.Join(details, "|") != strings.Join(tt.want, "|") {
			t.Errorf("%s: got %q, want %q", tt.name, details, tt.want)
		}
	}
}

func TestSeverity(t *testing.T) {
	for s, rank := range map[string]int{"info": 1, "warn": 2, "error": 3} {
		v, err := ParseSeverity(s)
		if err != nil || v.Rank() != rank {
			t.Errorf("ParseSeverity(%q) = %v, %v", s, v, err)
		}
	}
	if _, err := ParseSeverity("fatal"); err == nil {
		t.Error("unknown severity accepted")
	}
	if Series(cryptoMin.Series).TimeZone() != "UTC" || equityMin.TimeZone() != "America/New_York" {
		t.Error("time zones")
	}
	if cryptoMin.Series.String() != "crypto/coinbase/minute/btcusd" {
		t.Errorf("series = %s", cryptoMin.Series)
	}
}

// fullDay is 1440 identical minute bars.
func fullDay() []Bar {
	out := make([]Bar, 1440)
	for m := range out {
		out[m] = bar(m+1, m, "1", "1", "1", "1", "1")
	}
	return out
}

// withDay puts bars after a first bar at 00:00 and before a last bar at 23:59,
// so only the bars under test can produce findings.
func withDay(bars ...Bar) []Bar {
	out := append([]Bar{bar(1, 0, "1", "1", "1", "1", "1")}, bars...)
	var fill time.Duration
	for _, b := range out {
		if m := b.Time.Sub(d1016) / time.Minute; m > fill && m < 1440 {
			fill = m
		}
	}
	for m := int(fill) + 1; m < 1440; m++ {
		out = append(out, bar(len(out)+1, m, "1", "1", "1", "1", "1"))
	}
	return out
}

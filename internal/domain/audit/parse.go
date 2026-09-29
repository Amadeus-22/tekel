package audit

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// equityScale converts LEAN's US equity prices, stored in deci-cents
// (Data/equity/readme.md: "Divide prices by 10,000"), to dollars.
// maxExponent bounds scientific notation; real prices need far less.
const maxExponent = 30

var equityScale = big.NewRat(1, 10_000)

// Bar is one row of a LEAN trade-bar file. Prices are in the quote currency
// (equity deci-cents already converted). Values may be zero or negative: the
// parser accepts them so the checks can report them.
type Bar struct {
	Line                           int
	Time                           time.Time
	Open, High, Low, Close, Volume *big.Rat
}

// TradeTick is one row of a LEAN crypto trade tick file.
type TradeTick struct {
	Line     int
	Time     time.Time
	Price    *big.Rat
	Quantity *big.Rat
	// Suspicious is LEAN's optional fourth column (Common/Data/Market/Tick.cs).
	Suspicious bool
}

// ParseBar parses one trade-bar row of f:
//
//	second/minute: <ms since midnight>,open,high,low,close,volume
//	hour/daily:    <yyyyMMdd HH:mm>,open,high,low,close,volume
func ParseBar(f File, line int, text string) (Bar, error) {
	fields := strings.Split(text, ",")
	if len(fields) != 6 {
		return Bar{}, fmt.Errorf("%d fields, want 6", len(fields))
	}
	t, err := parseTime(f, fields[0])
	if err != nil {
		return Bar{}, err
	}
	vals := make([]*big.Rat, 5)
	for i := range vals {
		if vals[i], err = parseNumber(fields[i+1]); err != nil {
			return Bar{}, fmt.Errorf("field %d: %w", i+2, err)
		}
	}
	if f.SecurityType == Equity {
		for _, v := range vals[:4] {
			v.Mul(v, equityScale)
		}
	}
	return Bar{Line: line, Time: t, Open: vals[0], High: vals[1], Low: vals[2], Close: vals[3], Volume: vals[4]}, nil
}

// ParseTick parses one crypto trade tick row: <ms since midnight>,price,quantity[,suspicious].
func ParseTick(f File, line int, text string) (TradeTick, error) {
	fields := strings.Split(text, ",")
	if len(fields) != 3 && len(fields) != 4 {
		return TradeTick{}, fmt.Errorf("%d fields, want 3 or 4", len(fields))
	}
	t, err := parseTime(f, fields[0])
	if err != nil {
		return TradeTick{}, err
	}
	price, err := parseNumber(fields[1])
	if err != nil {
		return TradeTick{}, fmt.Errorf("field 2: %w", err)
	}
	qty, err := parseNumber(fields[2])
	if err != nil {
		return TradeTick{}, fmt.Errorf("field 3: %w", err)
	}
	return TradeTick{Line: line, Time: t, Price: price, Quantity: qty, Suspicious: len(fields) == 4 && fields[3] == "1"}, nil
}

// parseTime reads the first column. Dated files hold milliseconds since
// midnight, possibly fractional (LEAN multiplies by 10 000 ticks); the result is
// not range-checked here, CheckTime does that.
func parseTime(f File, s string) (time.Time, error) {
	if !f.Resolution.Dated() {
		t, err := time.Parse("20060102 15:04", s)
		if err != nil {
			return time.Time{}, fmt.Errorf("time %q: want yyyyMMdd HH:mm", s)
		}
		return t, nil
	}
	ms, err := parseNumber(s)
	if err != nil {
		return time.Time{}, fmt.Errorf("time %q: %w", s, err)
	}
	ns := new(big.Rat).Mul(ms, big.NewRat(int64(time.Millisecond), 1))
	if !ns.IsInt() || !ns.Num().IsInt64() {
		return time.Time{}, fmt.Errorf("time %q: not a whole number of nanoseconds", s)
	}
	return f.Date.Add(time.Duration(ns.Num().Int64())), nil
}

// parseNumber accepts an optionally signed decimal literal ("-1.5", "6.4e-05").
// Unlike domain.ParseDecimal it keeps negatives, which the checks report.
func parseNumber(s string) (*big.Rat, error) {
	if s == "" {
		return nil, fmt.Errorf("empty number")
	}
	if strings.IndexFunc(s, func(r rune) bool { return !strings.ContainsRune("0123456789.eE+-", r) }) >= 0 {
		return nil, fmt.Errorf("not a number: %q", s)
	}
	// Bound the exponent: big.Rat would otherwise build a huge number for "1e999999999".
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		if exp, err := strconv.Atoi(s[i+1:]); err != nil || exp > maxExponent || exp < -maxExponent {
			return nil, fmt.Errorf("not a number: %q", s)
		}
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return nil, fmt.Errorf("not a number: %q", s)
	}
	return r, nil
}

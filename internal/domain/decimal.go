package domain

import (
	"fmt"
	"math/big"
	"strings"
)

// maxDecimalPlaces bounds FormatRat. Exchange prices and quantities carry at most
// 8–10 fractional digits, so 30 leaves ample room while guaranteeing termination.
const maxDecimalPlaces = 30

// Decimal is an exact, non-negative decimal number as sent by an exchange.
//
// It keeps the exchange's original text (for audit and debugging) and an exact
// rational value (for arithmetic). The zero value is not valid; use ParseDecimal.
type Decimal struct {
	raw string
	rat *big.Rat
}

// ParseDecimal parses a non-negative decimal literal such as "0.00123000" or
// "6.4e-05". Only digits, one '.', and an optional exponent are accepted, so
// fractions ("1/3"), hex floats, NaN, Inf and negative values are rejected.
func ParseDecimal(s string) (Decimal, error) {
	if s == "" {
		return Decimal{}, fmt.Errorf("%w: empty", ErrInvalidDecimal)
	}
	if strings.IndexFunc(s, notDecimalRune) >= 0 || s[0] == '-' || s[0] == '+' {
		return Decimal{}, fmt.Errorf("%w: %q", ErrInvalidDecimal, s)
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return Decimal{}, fmt.Errorf("%w: %q", ErrInvalidDecimal, s)
	}
	return Decimal{raw: s, rat: r}, nil
}

func notDecimalRune(r rune) bool {
	return !strings.ContainsRune("0123456789.eE+-", r)
}

// MustDecimal is ParseDecimal for constants and tests; it panics on bad input.
func MustDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// String returns the exchange's original text.
func (d Decimal) String() string { return d.raw }

// Rat returns a copy of the exact value; callers may mutate it.
func (d Decimal) Rat() *big.Rat {
	if d.rat == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(d.rat)
}

// Float64 returns the nearest float64. Use it for statistics only, never for money.
func (d Decimal) Float64() float64 {
	if d.rat == nil {
		return 0
	}
	f, _ := d.rat.Float64()
	return f
}

// IsZero reports whether the value is zero (or the Decimal is unset).
func (d Decimal) IsZero() bool { return d.rat == nil || d.rat.Sign() == 0 }

// FormatRat renders r as the shortest plain decimal that represents it exactly
// ("17241.1", "0.00012", "5"), which is how LEAN's own data files spell numbers.
// Values without a terminating decimal expansion are rounded to maxDecimalPlaces;
// that cannot happen for sums of exchange decimals.
func FormatRat(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String()
	}
	scaled := new(big.Rat).Set(r)
	ten := big.NewRat(10, 1)
	for places := 1; places <= maxDecimalPlaces; places++ {
		scaled.Mul(scaled, ten)
		if scaled.IsInt() {
			return r.FloatString(places)
		}
	}
	return strings.TrimRight(strings.TrimRight(r.FloatString(maxDecimalPlaces), "0"), ".")
}

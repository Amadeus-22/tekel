package audit

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	datedName   = regexp.MustCompile(`^(\d{8})_([a-z]+)\.zip$`)
	undatedName = regexp.MustCompile(`^([a-z0-9]+)_([a-z]+)\.zip$`)
)

// ClassifyPath recognises a LEAN trade file from the end of its slash-separated
// path, so any root (the whole data folder or one symbol's folder) works:
//
//	crypto/<market>/{tick,second,minute}/<symbol>/<yyyymmdd>_trade.zip
//	crypto/<market>/{hour,daily}/<symbol>_trade.zip
//	equity/<market>/{second,minute}/<symbol>/<yyyymmdd>_trade.zip
//
// Anything else, including quote files, returns an error wrapping ErrUnsupported.
func ClassifyPath(path string) (File, error) {
	parts := strings.Split(path, "/")
	n := len(parts)
	at := func(i int) string { // i counts from the end: 1 is the file name
		if i > n {
			return ""
		}
		return parts[n-i]
	}
	name := at(1)

	if m := datedName.FindStringSubmatch(name); m != nil {
		sec, market, res, symbol := at(5), at(4), Resolution(at(3)), at(2)
		if err := supported(sec, res, m[2]); err != nil {
			return File{}, err
		}
		if !res.Dated() || market == "" || symbol == "" {
			return File{}, fmt.Errorf("%w: %s/%s/%s is not a dated layout", ErrUnsupported, sec, market, res)
		}
		date, err := time.Parse("20060102", m[1])
		if err != nil {
			return File{}, fmt.Errorf("%w: bad date in file name", ErrUnsupported)
		}
		return File{Series: Series{SecurityType: sec, Market: market, Resolution: res, Symbol: symbol}, Date: date}, nil
	}
	if m := undatedName.FindStringSubmatch(name); m != nil {
		sec, market, res := at(4), at(3), Resolution(at(2))
		if sec != Crypto || (res != Hour && res != Daily) || market == "" {
			return File{}, fmt.Errorf("%w: not a crypto hour/daily path", ErrUnsupported)
		}
		if err := supported(sec, res, m[2]); err != nil {
			return File{}, err
		}
		return File{Series: Series{SecurityType: sec, Market: market, Resolution: res, Symbol: m[1]}}, nil
	}
	return File{}, fmt.Errorf("%w: file name is not a LEAN trade layout", ErrUnsupported)
}

func supported(sec string, res Resolution, tickType string) error {
	if tickType != "trade" {
		return fmt.Errorf("%w: %s data (only trade data is audited)", ErrUnsupported, tickType)
	}
	switch {
	case sec == Crypto && (res.Dated() || res == Hour || res == Daily):
		return nil
	case sec == Equity && (res == Second || res == Minute):
		return nil
	}
	return fmt.Errorf("%w: %s %s", ErrUnsupported, sec, res)
}

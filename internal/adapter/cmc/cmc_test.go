package cmc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func serve(t *testing.T, status int, body string, check func(*http.Request)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if check != nil {
			check(r)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

// quotes_usd.json is a real response (trimmed to three coins per symbol):
// "BTC" names Bitcoin and look-alikes, and some ETH entries have no rank or price.
func TestQuotesPicksBestRankedCoin(t *testing.T) {
	fixture, err := os.ReadFile("testdata/quotes_usd.json")
	if err != nil {
		t.Fatal(err)
	}
	srv := serve(t, http.StatusOK, string(fixture), func(r *http.Request) {
		if r.URL.Path != "/v2/cryptocurrency/quotes/latest" {
			t.Errorf("path = %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("symbol"); got != "BTC,ETH" {
			t.Errorf("symbol = %q", got)
		}
		if got := r.URL.Query().Get("convert"); got != "USD" {
			t.Errorf("convert = %q", got)
		}
		if r.Header.Get("X-CMC_PRO_API_KEY") != "k" || strings.Contains(r.URL.RawQuery, "k=") {
			t.Error("key must travel in the header only")
		}
	})
	c, err := New("k", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Quotes(context.Background(), "USD", []string{"BTC", "ETH"})
	if err != nil {
		t.Fatal(err)
	}
	if p := got["BTC"].Price.String(); !strings.HasPrefix(p, "61606.") {
		t.Errorf("BTC = %s, want Bitcoin's price, not a look-alike's", p)
	}
	if p := got["ETH"].Price.String(); !strings.HasPrefix(p, "1740.") {
		t.Errorf("ETH = %s, want Ethereum's price", p)
	}
	if got["BTC"].UpdatedAt.IsZero() {
		t.Error("missing last_updated")
	}
}

func TestQuotesAPIErrorDoesNotLeakKey(t *testing.T) {
	srv := serve(t, http.StatusUnauthorized,
		`{"status":{"error_code":1002,"error_message":"API key missing."}}`, nil)
	c, _ := New("secret-key", srv.URL)
	_, err := c.Quotes(context.Background(), "USD", []string{"BTC"})
	if err == nil || !strings.Contains(err.Error(), "1002") {
		t.Fatalf("err = %v, want the API error code", err)
	}
	if strings.Contains(err.Error(), "secret-key") {
		t.Fatal("error leaks the API key")
	}
}

func TestQuotesBadBody(t *testing.T) {
	srv := serve(t, http.StatusOK, `not json`, nil)
	c, _ := New("k", srv.URL)
	if _, err := c.Quotes(context.Background(), "USD", []string{"BTC"}); err == nil {
		t.Fatal("want a decode error")
	}
}

func TestQuotesNoBasesMakesNoCall(t *testing.T) {
	srv := serve(t, http.StatusOK, `{}`, func(*http.Request) { t.Error("unexpected call") })
	c, _ := New("k", srv.URL)
	if got, err := c.Quotes(context.Background(), "USD", nil); err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestNewRequiresKey(t *testing.T) {
	if _, err := New("", ""); !errors.Is(err, ErrNoKey) {
		t.Fatalf("err = %v, want ErrNoKey", err)
	}
}

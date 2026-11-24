// Package cmc fetches reference prices from the CoinMarketCap Pro API
// (GET /v2/cryptocurrency/quotes/latest) and the key's credit usage
// (GET /v1/key/info, which costs no credits). It implements
// app.ReferenceSource and app.ReferenceUsageSource.
//
// One symbol can name many coins on CoinMarketCap ("BTC" returns Bitcoin and a
// dozen look-alikes, some unranked and without a price), so for each symbol the
// adapter keeps the coin with the best (lowest) cmc_rank.
package cmc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Amadeus-22/tekel/internal/domain"
	"github.com/Amadeus-22/tekel/internal/domain/reference"
)

// DefaultURL is the Pro API base URL.
const DefaultURL = "https://pro-api.coinmarketcap.com"

// maxBody caps how much of a response is read.
const maxBody = 4 << 20

// Client calls the CoinMarketCap Pro API.
type Client struct {
	// Key is sent in the X-CMC_PRO_API_KEY header, never in the URL, so it
	// cannot leak through an error message or a log line.
	Key string
	// BaseURL defaults to DefaultURL.
	BaseURL string
	HTTP    *http.Client
}

// status is the envelope every endpoint answers with.
type status struct {
	Status struct {
		ErrorCode    int    `json:"error_code"`
		ErrorMessage string `json:"error_message"`
	} `json:"status"`
}

type quotesResponse struct {
	status
	Data map[string][]coin `json:"data"`
}

type keyInfoResponse struct {
	status
	Data struct {
		Usage struct {
			CurrentDay struct {
				CreditsUsed int `json:"credits_used"`
			} `json:"current_day"`
			CurrentMonth struct {
				CreditsUsed int `json:"credits_used"`
				CreditsLeft int `json:"credits_left"`
			} `json:"current_month"`
		} `json:"usage"`
	} `json:"data"`
}

type coin struct {
	Name    string `json:"name"`
	CMCRank *int   `json:"cmc_rank"`
	Quote   map[string]struct {
		Price       *json.Number `json:"price"`
		LastUpdated time.Time    `json:"last_updated"`
	} `json:"quote"`
}

// Quotes implements app.ReferenceSource: one request for every base in convert.
func (c Client) Quotes(ctx context.Context, convert string, bases []string) (map[string]reference.Quote, error) {
	if len(bases) == 0 {
		return map[string]reference.Quote{}, nil
	}
	var body quotesResponse
	q := url.Values{"symbol": {strings.Join(bases, ",")}, "convert": {convert}}
	if err := c.get(ctx, "/v2/cryptocurrency/quotes/latest", q, &body, &body.status); err != nil {
		return nil, fmt.Errorf("cmc: quotes: %w", err)
	}
	return pick(body.Data, convert), nil
}

// Usage implements app.ReferenceUsageSource. /v1/key/info costs no credits.
func (c Client) Usage(ctx context.Context) (reference.Usage, error) {
	var body keyInfoResponse
	if err := c.get(ctx, "/v1/key/info", nil, &body, &body.status); err != nil {
		return reference.Usage{}, fmt.Errorf("cmc: key info: %w", err)
	}
	u := body.Data.Usage
	return reference.Usage{
		UsedToday: u.CurrentDay.CreditsUsed, UsedMonth: u.CurrentMonth.CreditsUsed, LeftMonth: u.CurrentMonth.CreditsLeft,
	}, nil
}

// get sends one authenticated GET and decodes the JSON answer into out; st is
// out's embedded status, checked for an API error.
func (c Client) get(ctx context.Context, path string, q url.Values, out any, st *status) error {
	base := c.BaseURL
	if base == "" {
		base = DefaultURL
	}
	u := strings.TrimRight(base, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-CMC_PRO_API_KEY", c.Key)
	req.Header.Set("Accept", "application/json")
	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(out); err != nil {
		return fmt.Errorf("HTTP %d: decode: %w", resp.StatusCode, err)
	}
	if resp.StatusCode/100 != 2 || st.Status.ErrorCode != 0 {
		return fmt.Errorf("HTTP %d, error %d: %s", resp.StatusCode, st.Status.ErrorCode, st.Status.ErrorMessage)
	}
	return nil
}

// pick keeps, per symbol, the best-ranked coin that has a price in convert.
func pick(data map[string][]coin, convert string) map[string]reference.Quote {
	out := map[string]reference.Quote{}
	for symbol, coins := range data {
		best := -1
		var quote reference.Quote
		for _, c := range coins {
			q, ok := c.Quote[convert]
			if !ok || q.Price == nil || c.CMCRank == nil {
				continue
			}
			price, err := domain.ParseDecimal(q.Price.String())
			if err != nil || price.IsZero() {
				continue
			}
			if best < 0 || *c.CMCRank < best {
				best, quote = *c.CMCRank, reference.Quote{Price: price, UpdatedAt: q.LastUpdated}
			}
		}
		if best >= 0 {
			out[strings.ToUpper(symbol)] = quote
		}
	}
	return out
}

// ErrNoKey is returned by New when the API key is empty.
var ErrNoKey = errors.New("cmc: API key is required")

// New returns a client with a request timeout.
func New(key, baseURL string) (Client, error) {
	if key == "" {
		return Client{}, ErrNoKey
	}
	return Client{Key: key, BaseURL: baseURL, HTTP: &http.Client{Timeout: 15 * time.Second}}, nil
}

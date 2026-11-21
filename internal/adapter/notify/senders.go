package notify

import (
	"bytes"
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
)

// Webhook posts to an incoming webhook. The body is picked by host: Slack
// takes {"text"}, Discord {"content"}, anything else gets {"text"} plus the
// incident as structured fields, for your own receiver.
type Webhook struct {
	URL    string
	Client *http.Client
}

// Name implements Sender.
func (w Webhook) Name() string { return "webhook" }

// Send implements Sender.
func (w Webhook) Send(ctx context.Context, inc domain.Incident, text string) error {
	u, err := url.Parse(w.URL)
	if err != nil {
		return fmt.Errorf("webhook url: %w", err)
	}
	var body any
	switch host := strings.ToLower(u.Hostname()); {
	case host == "hooks.slack.com":
		body = map[string]string{"text": text}
	case host == "discord.com" || host == "discordapp.com":
		body = map[string]string{"content": text}
	default:
		body = genericPayload{Text: text, Incident: payloadIncident(inc)}
	}
	return postJSON(ctx, w.Client, w.URL, body, "webhook")
}

type genericPayload struct {
	Text     string          `json:"text"`
	Incident incidentPayload `json:"incident"`
}

type incidentPayload struct {
	Exchange   string    `json:"exchange"`
	Symbol     string    `json:"symbol"`
	Kind       string    `json:"kind"`
	DetectedAt time.Time `json:"detected_at"`
	Start      time.Time `json:"start,omitzero"`
	End        time.Time `json:"end,omitzero"`
	Detail     string    `json:"detail,omitempty"`
}

func payloadIncident(inc domain.Incident) incidentPayload {
	return incidentPayload{Exchange: inc.Exchange, Symbol: inc.Symbol, Kind: string(inc.Kind),
		DetectedAt: inc.DetectedAt.UTC(), Start: inc.Start.UTC(), End: inc.End.UTC(), Detail: inc.Detail}
}

// TelegramAPI is the Bot API base URL; tests point it at a local server.
const TelegramAPI = "https://api.telegram.org"

// Telegram sends with a bot to one chat.
type Telegram struct {
	Token  string
	ChatID string
	// API defaults to TelegramAPI.
	API    string
	Client *http.Client
}

// Name implements Sender.
func (t Telegram) Name() string { return "telegram" }

// Send implements Sender.
func (t Telegram) Send(ctx context.Context, _ domain.Incident, text string) error {
	api := t.API
	if api == "" {
		api = TelegramAPI
	}
	body := map[string]any{"chat_id": t.ChatID, "text": text, "disable_web_page_preview": true}
	// The token is part of the URL: postJSON never puts the URL in an error.
	return postJSON(ctx, t.Client, api+"/bot"+t.Token+"/sendMessage", body, "telegram")
}

// postJSON posts body and fails on a non-2xx answer. Errors name the sender,
// never the URL, which may carry a secret.
func postJSON(ctx context.Context, c *http.Client, target string, body any, name string) error {
	if c == nil {
		c = http.DefaultClient
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("%s: encode: %w", name, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("%s: build request", name)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err // drop the URL
		}
		return fmt.Errorf("%s: post: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return fmt.Errorf("%s: HTTP %d: %s", name, resp.StatusCode, bytes.TrimSpace(msg))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

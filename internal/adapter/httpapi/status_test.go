package httpapi

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func fetch(t *testing.T, url string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestStatusPageAndAssets(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	for _, tc := range []struct{ path, ctype, contains string }{
		{"/", "text/html", "/assets/app.js"},
		{"/assets/app.js", "javascript", "/v1/streams"},
		{"/assets/style.css", "text/css", "--bg"},
	} {
		resp, body := fetch(t, srv.URL+tc.path)
		if resp.StatusCode != 200 || !strings.Contains(resp.Header.Get("Content-Type"), tc.ctype) || !strings.Contains(body, tc.contains) {
			t.Errorf("GET %s = %d %q, body has %q: %v", tc.path, resp.StatusCode, resp.Header.Get("Content-Type"), tc.contains, strings.Contains(body, tc.contains))
		}
		csp := resp.Header.Get("Content-Security-Policy")
		if !strings.Contains(csp, "default-src 'none'") || strings.Contains(csp, "unsafe-inline") {
			t.Errorf("GET %s CSP = %q, want a strict policy", tc.path, csp)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("GET %s without nosniff", tc.path)
		}
	}
}

func TestStatusPageLoadsNothingFromOtherOrigins(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	for _, p := range []string{"/", "/assets/app.js", "/assets/style.css"} {
		_, body := fetch(t, srv.URL+p)
		for _, bad := range []string{"<script>", "innerHTML", "src=\"http", "href=\"http://", "@import", "fonts.googleapis"} {
			if strings.Contains(body, bad) {
				t.Errorf("%s contains %q", p, bad)
			}
		}
	}
}

func TestAssetsHaveNoListingAndAPIErrorsStayJSON(t *testing.T) {
	srv := newServer(t, &fakeIncidents{}, nil)
	if resp, _ := fetch(t, srv.URL+"/assets/"); resp.StatusCode != 404 {
		t.Errorf("GET /assets/ = %d, want 404 (no listing)", resp.StatusCode)
	}
	if code, body := get(t, srv.URL+"/v2/nothing"); code != 404 || errorCode(body) != "not_found" {
		t.Errorf("unknown API route = %d %v", code, body)
	}
}

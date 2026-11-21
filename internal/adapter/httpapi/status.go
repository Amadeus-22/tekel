package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// web holds the status page: plain HTML, CSS and JavaScript that read
// /v1/streams and /v1/incidents. Nothing is loaded from another origin.
//
//go:embed web
var web embed.FS

// pageCSP allows only this origin; the page has no inline script or style.
const pageCSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

// statusPage serves / (the page) and /assets/* (its files).
func statusPage() http.Handler {
	files, err := fs.Sub(web, "web")
	if err != nil {
		panic(err) // the embedded directory always exists
	}
	server := http.FileServerFS(files)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", pageCSP)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		if r.URL.Path == "/" {
			http.ServeFileFS(w, r, files, "index.html")
			return
		}
		if strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r) // no directory listings
			return
		}
		http.StripPrefix("/assets/", server).ServeHTTP(w, r)
	})
}
